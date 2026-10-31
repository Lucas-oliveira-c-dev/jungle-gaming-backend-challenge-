import { loadConfig } from '../src/config';
import { createSqsClient, ensureQueues } from '../src/infrastructure/messaging/sqs';

const config = loadConfig();
const sqs = createSqsClient(config.sqs);
console.log(JSON.stringify(await ensureQueues(sqs, config.sqs)));
sqs.destroy();
