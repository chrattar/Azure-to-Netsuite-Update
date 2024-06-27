/**
 * @NApiVersion 2.x
 * @NScriptType ScheduledScript
 */
define(['N/https', 'N/file', 'N/task'], function(https, file, task) {

    function execute(context) {
        try {
            var sasUrl = 'azure.microsoft'; // Replace with the actual SAS URL

            // GET request -> SAS URL for dl
            var response = https.get({
                url: sasUrl
            });

            if (response.code === 200) {
                // Save to File Cabinet
                var csvFile = file.create({
                    name: 'extracted_data.csv',
                    fileType: file.Type.CSV,
                    contents: response.body,
                    folder: -15 // File Cabinent which is storing all the requiste JS SuiteScripts

                var fileId = csvFile.save();
                log.debug('File saved to File Cabinet', 'File ID: ' + fileId);

                // create task for import
                var csvImportTask = task.create({
                    taskType: task.TaskType.CSV_IMPORT,
                    importFile: file.load({ id: fileId }),
                    mappingId: 44533 //PULL Internal ID for the custom import map that was created in NS
                });

                // submit task for import
                var taskId = csvImportTask.submit();
                log.debug('CSV Import Task Submitted', 'Task ID: ' + taskId);
            } else {
                log.error('Failed to download file', 'Response Code: ' + response.code);
            }
        } catch (e) {
            log.error('Error in CSV Import Task', e.toString());
        }
    }

    return {
        execute: execute
    };
});
