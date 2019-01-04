const openWhiskExtractor = require('./openWhiskExtractor');
const awsWorkflowExtractor = require('./awsWorkflowExtractor');
const awsHintExtractor = require('./awsHintExtractor');

let startAt = 60000 * 25;
let testname = "";
if (process.argv[2]) {
    testname = process.argv[2]
}
if (process.argv[3]) {
    startAt = 60000 * Number.parseInt(process.argv[3])
}

let logsPerMinute = 2400;
awsWorkflowExtractor.collectAwsWorkflowLogs(logsPerMinute, startAt, testname);
awsHintExtractor.collectAwsHintLogs(logsPerMinute, startAt, testname);
openWhiskExtractor.collectOpenWhiskLogs(logsPerMinute, startAt, testname);


