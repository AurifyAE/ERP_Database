import { fileURLToPath } from 'url';
import path from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export const config = {
    DB_NAME: 'BULLIONDEMO',
    REFRESH_INTERVAL: 3 * 60 * 1000, // 2 minutes in milliseconds
    MAX_RETRY_ATTEMPTS: 5,
    RETRY_DELAY: 10000, // 20 seconds
    LOG_FILE_PATH: path.join(__dirname, 'database_manager.log'),
    SOURCE_PATH: 'D:\\SQLDB\\BULLIONDEMO.mdf',
    DESTINATION_FOLDER: 'D:\\ERP',
    dbConfig: {
        user: 'sa',
        password: 'Aurify-bullions',
        server: 'localhost\\SQLEXPRESS',
        database: 'master',
        options: {
            encrypt: true,
            trustServerCertificate: true,
            connectionTimeout: 30000,
            requestTimeout: 30000
        },
        pool: {
            max: 10,
            min: 0,
            idleTimeoutMillis: 30000
        }
    }
};