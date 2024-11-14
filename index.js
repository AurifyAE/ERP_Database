import { promises as fs } from 'fs';
import path from 'path';
import { exec } from 'child_process';
import { promisify } from 'util';
import crypto from 'crypto';
import sql from 'mssql';
import { fileURLToPath } from 'url';
import { config } from './database-config.js';

const execAsync = promisify(exec);
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const {
    DB_NAME,
    REFRESH_INTERVAL,
    MAX_RETRY_ATTEMPTS,
    RETRY_DELAY,
    LOG_FILE_PATH,
    SOURCE_PATH,
    DESTINATION_FOLDER,
    dbConfig
} = config;

let isRefreshInProgress = false;
let sqlPool = null;

// Initialize SQL Pool
async function initializeSqlPool() {
    try {
        if (!sqlPool) {
            sqlPool = await sql.connect(dbConfig);
            await logMessage("SQL Pool initialized successfully");
        }
    } catch (error) {
        await logMessage(`Failed to initialize SQL Pool: ${error.message}`, true);
        throw error;
    }
}

// Logging utility
async function logMessage(message, isError = false) {
    const timestamp = new Date().toISOString();
    const logEntry = `${timestamp} - ${isError ? 'ERROR' : 'INFO'} - ${message}\n`;
    
    try {
        await fs.appendFile(LOG_FILE_PATH, logEntry);
        console.log(logEntry.trim());
    } catch (error) {
        console.error(`Failed to write to log file: ${error.message}`);
    }
}

// Retry utility
async function withRetry(operation, operationName, maxAttempts = MAX_RETRY_ATTEMPTS) {
    let lastError;
    
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            await logMessage(`Attempting ${operationName} (Attempt ${attempt}/${maxAttempts})`);
            const result = await operation();
            await logMessage(`${operationName} completed successfully`);
            return result;
        } catch (error) {
            lastError = error;
            await logMessage(`${operationName} failed: ${error.message}`, true);
            
            if (attempt < maxAttempts) {
                await logMessage(`Waiting ${RETRY_DELAY/1000} seconds before retry...`);
                await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
            }
        }
    }
    
    throw new Error(`${operationName} failed after ${maxAttempts} attempts. Last error: ${lastError.message}`);
}

// File operations
async function checkFileAccessibility(filePath) {
    try {
        await fs.access(filePath, fs.constants.R_OK);
        return true;
    } catch {
        return false;
    }
}

async function calculateFileHash(filePath) {
    return withRetry(async () => {
        const fileBuffer = await fs.readFile(filePath);
        const hashSum = crypto.createHash('sha256');
        hashSum.update(fileBuffer);
        return hashSum.digest('hex');
    }, 'File hash calculation');
}

async function copyFileWithVSS(sourceFile, destinationPath) {
    return withRetry(async () => {
        if (!(await checkFileAccessibility(sourceFile))) {
            throw new Error(`Source file ${sourceFile} is not accessible`);
        }

        const shadowPathCommand = `powershell.exe -Command "
            $ErrorActionPreference = 'Stop'
            try {
                $shadowId = (Get-WmiObject -List Win32_ShadowCopy).Create('${path.dirname(sourceFile)}', 'ClientAccessible').ShadowID
                $shadow = Get-WmiObject Win32_ShadowCopy | Where-Object { $_.ID -eq $shadowId }
                $deviceObject = $shadow.DeviceObject + '\\'
                $volume = (Get-WmiObject -Class Win32_Volume | Where-Object { $_.DeviceID -eq $shadow.VolumeName }).DriveLetter
                $filePath = '${sourceFile}' -replace '^[A-Za-z]:', $volume
                $shadowPath = $deviceObject + $filePath.Substring(2)
                Write-Output $shadowPath
            } catch {
                Write-Error $_.Exception.Message
                exit 1
            }
        "`;

        const { stdout: shadowPath } = await execAsync(shadowPathCommand);
        if (!shadowPath.trim()) throw new Error("Shadow path creation failed");

        await fs.mkdir(path.dirname(destinationPath), { recursive: true });
        await fs.copyFile(shadowPath.trim(), destinationPath);
        
        // Clean up VSS
        await execAsync(`powershell.exe -Command "
            Get-WmiObject Win32_ShadowCopy | 
            Where-Object { $_.DeviceObject -eq '${shadowPath.trim().split('\\')[0]}' } | 
            ForEach-Object { $_.Delete() }
        "`);
        
        return true;
    }, 'VSS copy operation');
}

async function fallbackCopy(sourceFile, destinationPath) {
    return withRetry(async () => {
        if (!(await checkFileAccessibility(sourceFile))) {
            throw new Error(`Source file ${sourceFile} is not accessible`);
        }

        await fs.mkdir(path.dirname(destinationPath), { recursive: true });
        await fs.copyFile(sourceFile, destinationPath);
        return true;
    }, 'Fallback copy operation');
}

// Database operations
async function checkDatabaseExists() {
    return withRetry(async () => {
        await initializeSqlPool();
        const result = await sqlPool.request()
            .query(`
                SELECT database_id 
                FROM sys.databases 
                WHERE name = '${DB_NAME}'
            `);
        return result.recordset.length > 0;
    }, 'Database existence check');
}

async function dropDatabase() {
    return withRetry(async () => {
        await initializeSqlPool();
        
        const exists = await checkDatabaseExists();
        if (!exists) {
            await logMessage(`Database ${DB_NAME} does not exist, skipping drop operation`);
            return true;
        }

        try {
            await sqlPool.request().query(`
                IF EXISTS (SELECT name FROM sys.databases WHERE name = '${DB_NAME}')
                BEGIN
                    ALTER DATABASE ${DB_NAME} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE ${DB_NAME};
                END
            `);
            await logMessage(`Database ${DB_NAME} dropped successfully`);
            return true;
        } catch (error) {
            throw new Error(`Failed to drop database: ${error.message}`);
        }
    }, 'Database drop operation');
}

async function createDatabase(mdfPath) {
    return withRetry(async () => {
        if (!(await checkFileAccessibility(mdfPath))) {
            throw new Error(`MDF file ${mdfPath} is not accessible`);
        }

        await initializeSqlPool();

        try {
            // First attempt to create with FOR ATTACH_REBUILD_LOG
            await sqlPool.request().query(`
                CREATE DATABASE ${DB_NAME} ON 
                (FILENAME = '${mdfPath}')
                FOR ATTACH_REBUILD_LOG;
            `);
            await logMessage(`Database ${DB_NAME} created successfully with FOR ATTACH_REBUILD_LOG`);
        } catch (error) {
            // Check if the error is related to opening the new database
            if (error.message.includes("Could not open new database")) {
                await logMessage(`Standard attach failed. Retrying with FOR ATTACH_FORCE_REBUILD_LOG`, true);

                // Retry with FOR ATTACH_FORCE_REBUILD_LOG
                await sqlPool.request().query(`
                    CREATE DATABASE ${DB_NAME} ON 
                    (FILENAME = '${mdfPath}')
                    FOR ATTACH_FORCE_REBUILD_LOG;
                `);
                await logMessage(`Database ${DB_NAME} created successfully with FOR ATTACH_FORCE_REBUILD_LOG`);
            } else {
                throw error; // rethrow if it's a different error
            }
        }

        return true;
    }, 'Database creation');
}


async function copyAndPrepareDatabase() {
    const destinationPath = path.join(DESTINATION_FOLDER, path.basename(SOURCE_PATH));
    await logMessage("Starting database file copy process...");

    try {
        if (!(await checkFileAccessibility(SOURCE_PATH))) {
            throw new Error("Source database file is not accessible");
        }

        let copySuccess = false;
        try {
            copySuccess = await copyFileWithVSS(SOURCE_PATH, destinationPath);
        } catch (vssError) {
            await logMessage("VSS copy failed, attempting fallback copy...");
            copySuccess = await fallbackCopy(SOURCE_PATH, destinationPath);
        }

        if (copySuccess) {
            const fileHash = await calculateFileHash(SOURCE_PATH);
            await logMessage("Database file copied successfully!");
            return {
                success: true,
                destinationPath,
                fileHash
            };
        }

        return { success: false };
    } catch (error) {
        await logMessage(`Error in file copy process: ${error.message}`, true);
        return { success: false };
    }
}

async function refreshDatabase() {
    if (isRefreshInProgress) {
        await logMessage("Another refresh operation is in progress. Skipping this cycle.");
        return false;
    }

    isRefreshInProgress = true;
    await logMessage("Starting database refresh process...");
    
    try {
        const dbExists = await checkDatabaseExists();
        
        if (dbExists) {
            await logMessage("Existing database found. Dropping...");
            await dropDatabase();
        }

        const copyResult = await copyAndPrepareDatabase();
        if (copyResult.success) {
            await createDatabase(copyResult.destinationPath);
            await logMessage("Database refreshed successfully!");
            return true;
        }
        
        await logMessage("Failed to refresh database.", true);
        return false;
    } catch (error) {
        await logMessage(`Error in database refresh process: ${error.message}`, true);
        return false;
    } finally {
        isRefreshInProgress = false;
    }
}

async function cleanup() {
    try {
        if (sqlPool) {
            await sqlPool.close();
            await logMessage("SQL Pool closed successfully");
        }
    } catch (error) {
        await logMessage(`Error during cleanup: ${error.message}`, true);
    }
}

async function startDatabaseManagement() {
    process.on('SIGINT', async () => {
        await logMessage("Received SIGINT. Cleaning up...");
        await cleanup();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        await logMessage("Received SIGTERM. Cleaning up...");
        await cleanup();
        process.exit(0);
    });

    process.on('uncaughtException', async (error) => {
        await logMessage(`Uncaught Exception: ${error.message}`, true);
        await cleanup();
        process.exit(1);
    });

    process.on('unhandledRejection', async (reason, promise) => {
        await logMessage(`Unhandled Rejection at: ${promise}\nReason: ${reason}`, true);
    });

    try {
        await initializeSqlPool();
        await refreshDatabase();
        
        setInterval(async () => {
            try {
                await logMessage("\nStarting scheduled database refresh...");
                await refreshDatabase();
            } catch (error) {
                await logMessage(`Scheduled refresh failed: ${error.message}`, true);
            }
        }, REFRESH_INTERVAL);
        
        await logMessage(`Database refresh scheduler started. Will refresh every ${REFRESH_INTERVAL/1000} seconds.`);
    } catch (error) {
        await logMessage(`Failed to start database management: ${error.message}`, true);
        process.exit(1);
    }
}

// Start the process
startDatabaseManagement().catch(async (error) => {
    await logMessage(`Fatal error in database management: ${error.message}`, true);
    await cleanup();
    process.exit(1);
});