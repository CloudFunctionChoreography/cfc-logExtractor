'use strict';

const json2xls = require('json2xls');
const fs = require('file-system');
const openwhisk = require('openwhisk');
const ow = openwhisk({
    apihost: 'openwhisk.eu-gb.bluemix.net',
    namespace: 'simon.buchholz@campus.tu-berlin.de_dev',
    api_key: '735bf87f-b685-4dad-a705-b6e48e006cb3:FSRxx0LxHC0ibdttIylmH2O20R5TIaIi3FVnm6uej2aiYO8y8APMelNYcbuh88OC'
});
const now = new Date().getTime();

const actionNames = ['federated16stepsaction1Handler',
    'federated16stepsaction2Handler',
    'federated16stepsaction3Handler',
    'federated16stepsaction4Handler',
    'federated16stepsaction9Handler',
    'federated16stepsaction10Handler',
	'federated16stepsaction11Handler',
    'federated16stepsaction12Handler'
];

const timeout = 0;
let storeTestname = "";

const collectLogs = (actionName, startAt) => {
    // 1. Get all activation ids
    const getAllActivationsPromise = () => {
        return new Promise((resolve, reject) => {
            let retryCounter = 0;
            let since = now - startAt;
            console.log(`Start getting OW logs for ${actionName}, since ${since}, upto ${now}`);
            let allActivations = [];
            const recursiveCallback = () => {
                let upto = since + 8000;
                collectOpenWhiskLogsForAction(actionName, since, upto).then(activations => {
                    retryCounter = 0;
                    allActivations = [...allActivations, ...activations];
                    since += 8000;
                    if (since < now) {
                        setTimeout(() => {
                            recursiveCallback();
                        }, timeout);
                    } else {
                        resolve(allActivations)
                    }
                }).catch(error => {
                    while (retryCounter < 50) {
						setTimeout(() => {
								recursiveCallback();
								retryCounter++;
							}, retryCounter * 3000);
                    }
                    if (retryCounter === 50) {
                        reject(error);
                    }
                });
            };
            recursiveCallback();
        });
    };

    // 2. Get activation details for all activation ids
    const getActivationsPromise = () => {
        return new Promise((resolve, reject) => {
            getAllActivationsPromise().then(allActivations => {
                console.log(`Identified ${allActivations.length} entries for ${actionName}`)
                let detailedActionActivations = [];
                console.log(`${detailedActionActivations.length} activations processed for ${actionName}`);
                let logInterval = setInterval(() => {
                    console.log(`${detailedActionActivations.length} activations processed for ${actionName}`);
                }, 30000);
                const recursiveCallback = () => {
                    let activation = allActivations.pop();
                    if (activation) {
                        let retryCounter = 0;
                        const getSingleActionFunction = () => {
                            getSingleAction(activation.activationId, 0).then(singleActivation => {
                                retryCounter = 0;
                                detailedActionActivations.push(singleActivation);
                                setTimeout(() => {
                                    recursiveCallback();
                                }, timeout);
                            }).catch(reason => {
                                while (retryCounter < 50) {
									setTimeout(() => {
										getSingleActionFunction();
										retryCounter++;
									}, retryCounter * 3000);
                                }
                                if (retryCounter === 50) {
                                    clearInterval(logInterval);
                                    reject(reason)
                                }
                            })
                        };
                        getSingleActionFunction();
                    } else {
                        clearInterval(logInterval);
                        resolve(detailedActionActivations)
                    }
                };
                recursiveCallback();
            }).catch(reason => {
                reject(reason)
            });
        });
    };

    // 3. Transform activation details and store them in xlsx
    return new Promise((resolve, reject) => {
        getActivationsPromise().then(detailedActionActivations => {
            let stepLogResults = [];
            let hintLogResults = [];
            for (let activation of detailedActionActivations) {
				let transformedLog;
				if (activation) { transformedLog = transformLogs(activation); }
				if (transformedLog && transformedLog.stepLog) stepLogResults.push(transformedLog.stepLog);
                else if (transformedLog && transformedLog.hintLog) hintLogResults.push(transformedLog.hintLog);
            }

            if (stepLogResults.length > 0) {
                allStepLogs = [...allStepLogs, ...stepLogResults];

                let xlsStepLogs = json2xls(stepLogResults);
                let location = `benchmark/stepLogs/step_log_openWhisk_${actionName}.xlsx`;
                if (storeTestname !== "") location = `benchmark/${storeTestname}/stepLogs/step_log_openWhisk_${actionName}.xlsx`;
                fs.writeFileSync(location, xlsStepLogs, 'binary');
                console.log(`+++++++++++++++++ Added ${stepLogResults.length} new log entries to xls: ${location} ++++++++++++++++++`);

                let xlsStepLogsAll = json2xls(allStepLogs);
                let locationAll = `benchmark/combinedOpenWhiskStepLogs.xlsx`;
                if (storeTestname !== "") locationAll = `benchmark/${storeTestname}/combinedOpenWhiskStepLogs.xlsx`;
                fs.writeFileSync(locationAll, xlsStepLogsAll, 'binary');
                console.log(`+++++++++++++++++ Updated ${allStepLogs.length} log entries to xls: ${locationAll} ++++++++++++++++++`);
            } else {
                console.log(`+++++++++++++++++ Skipped creation of file since ${stepLogResults.length} new step log entries for ${actionName} ++++++++++++++++++`);
            }

            if (hintLogResults.length > 0) {
                allHintLogs = [...allHintLogs, ...hintLogResults];

                let xlsHintLogs = json2xls(hintLogResults);
                let location2 = `benchmark/hintLogs/hint_log_openWhisk_${actionName}.xlsx`;
                if (storeTestname !== "") location2 = `benchmark/${storeTestname}/hintLogs/hint_log_openWhisk_${actionName}.xlsx`;
                fs.writeFileSync(location2, xlsHintLogs, 'binary');
                console.log(`+++++++++++++++++ Added ${hintLogResults.length} new log entries to xls: ${location2} ++++++++++++++++++`)

                let xlsHintLogsAll = json2xls(allHintLogs);
                let location2All = `benchmark/combinedOpenWhiskHintLogs.xlsx`;
                if (storeTestname !== "") location2All = `benchmark/${storeTestname}/combinedOpenWhiskHintLogs.xlsx`;
                fs.writeFileSync(location2All, xlsHintLogsAll, 'binary');
                console.log(`+++++++++++++++++ Updated ${allHintLogs.length} log entries to xls: ${location2All} ++++++++++++++++++`)
            } else {
                console.log(`+++++++++++++++++ Skipped creation of file since ${hintLogResults.length} new hint log entries for ${actionName} ++++++++++++++++++`);
            }
            resolve(`Logs for ${actionName} stored`)


        }).catch(reason => {
            reject(reason)
        })
    });
};
var allHintLogs = [];
var allStepLogs = [];

const collectOpenWhiskLogsForAction = (actionName, since, upto) => {
    return new Promise((resolve, reject) => {
        let counter = 0;
		
        const getActivationsList = () => {
            ow.activations.list({
                name: actionName,
                since: since,
                upto: upto,
                limit: 200
            }).then(activationsList => {
				if (activationsList) {
					if (activationsList.length >= 200) console.warn("activation list length too long");
					if (activationsList.length === 0 && counter < 1) { // TODO adjust the counter
						counter++; // TODO because of couchDB returning stale results we retry it
						getActivationsList();
					} else {
						resolve(activationsList)
					}
				} else {
					getActivationsList();
				}
            }).catch(error => {
                console.error(error)
                reject(error)
            });
        }
		
		try {
			getActivationsList();
		} catch (e) {
			console.warn(e); // pass exception object to error handler
			setTimeout(() => {
				getActivationsList();
			}, 1000);
		}
    })
};

const getSingleAction = (activationId) => {
    return new Promise((resolve, reject) => {
        const getActivation = () => { ow.activations.get({
            activationId: activationId
        }).then(result => {
            resolve(result)
        }).catch(error => {
            console.error(error)
            reject(error)
        })
		}
		
		try {
			getActivation();
		} catch (e) {
			console.warn(e); // pass exception object to error handler
			setTimeout(() => {
				getActivation();
			}, 1000);
		}
    })
};

const transformLogs = (activation) => {
    for (let log of activation.logs) {
        if (log.includes("LOG_WORKFLOW_STATE:")) {
            let logWorkflowState = JSON.parse(log.split("LOG_WORKFLOW_STATE:")[1]);

            let historyItem;
            for (let item of logWorkflowState.excutionHistory) {
                if (item.step === logWorkflowState.currentStep) {
                    historyItem = item;
                    break;
                }
            }

            if (!historyItem) {
                console.error('No history item found for current step')
                return null;
            } else {
                let waitTime = 0;
                let initTime = 0;
                let memorySize = -1;
                const annotations = activation.annotations;
                annotations.forEach(annotation => {
                    if (annotation.key === "limits") {
                        memorySize = annotation.value.memory;
                    } else if (annotation.key === "waitTime") {
                        waitTime = annotation.value
                    } else if (annotation.key === "initTime") {
                        initTime = annotation.value
                    }
                });

                let duration = Number(activation.duration);
                const initDuration = waitTime + initTime;
                const executionDuration = duration - initTime;
                let actionActivationId = activation.activationId;
                let billedDuration = false;
                let maxMemoryUsed = false;
                let coldExecution = historyItem.stateProperties.coldExecution ? 1 : 0;
                let activationLog = {
                    executionUuid: logWorkflowState.executionUuid,
                    functionRequestId: actionActivationId,
                    billedDuration: billedDuration,
                    coldExecution: coldExecution,
                    context: logWorkflowState.excutionHistory[0].stateProperties.context,
                    initDuration: initDuration,
                    executionDuration: duration - initTime,
                    duration: executionDuration + initDuration,
                    faasDuration: duration, // duration measured by the faas system (in ow this excludes the waittime)
                    functionInstanceId: historyItem.stateProperties.functionInstanceUuid,
                    maxMemoryUsed: maxMemoryUsed,
                    provider: historyItem.provider,
                    step: historyItem.step,
                    memorySize: memorySize,
                    // logWorkflowState: logWorkflowState,
                    optimizationMode: logWorkflowState.optimizationMode,
                };

                return {stepLog: activationLog};
            }
        } else if (log.includes("LOG_WORKFLOW_HINT:")) {
            let logHintObject = JSON.parse(log.split("LOG_WORKFLOW_HINT:")[1]);
            let triggeredFrom = logHintObject.triggeredFrom;
            delete logHintObject.triggeredFrom;


            let duration = Number(activation.duration);
            let memorySize = activation.annotations[0].value.memory; // TODO does not always work because annotations is different for activations
            let billedDuration = false;
            let maxMemoryUsed = false;

            let activationLog = {
                billedDuration: billedDuration,
                duration: duration,
                maxMemoryUsed: maxMemoryUsed,
                memorySize: memorySize,
                triggeredFromFunctionExecutionId: triggeredFrom.functionExecutionId,
                triggeredFromFunctionInstanceUuid: triggeredFrom.functionInstanceUuid,
                triggeredFromStep: triggeredFrom.step,
                recursiveHintCounter: logHintObject.recursiveHintCounter
            };

            return {hintLog: Object.assign(logHintObject, activationLog)};

        }
    }
};


const collectOpenWhiskLogs = (logsPerMinute, startAt, testname) => {
    storeTestname = testname;

    const recursiveCallback = () => {
        let actionName = actionNames.pop();
        if (actionName) {
            collectLogs(actionName, startAt).then(result => {
                // console.log(result);
                recursiveCallback();
            }).catch(reason => {
                console.error(reason);
                recursiveCallback();
            })
        }
    };
    recursiveCallback();
};

exports.collectOpenWhiskLogs = collectOpenWhiskLogs;