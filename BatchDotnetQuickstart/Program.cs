using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Batch.Conventions.Files;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace BatchDotNetQuickstart
{
    public class Program
    {
        // Update the Batch and Storage account credential strings below with the values unique to your accounts.
        // These are used when constructing connection strings for the Batch and Storage client objects.
        
        // Batch account credentials
        private const string BatchAccountName = "emergentai";
        private const string BatchAccountKey = "xueHGH1qwm+bWjFYtj4mdzCLXxGZBUAdHhddWFIvd/fgaE3a0z9y0iN5RFGMY/AErEbXjJIwIUa4lrWeABcNLw==";
        private const string BatchAccountUrl = "https://emergentai.uksouth.batch.azure.com";

        // Storage account credentials
        private const string StorageAccountName = "emergentai";
        private const string StorageAccountKey = "FqOL31fHPNRhRW3jpLAeL8FF+sfyZqYEtUqzMpJo257CCSIleJKusGihiNxOqqwV4b/JSq+srU23S5C2Ao9dRw==";

        // Batch resource settings
        private const string PoolId = "DotNetQuickstartPool";
        private const string JobId = "DotNetQuickstartJob";
        private const int PoolNodeCount = 2;
        private const string PoolVMSize = "STANDARD_A1_v2";


        static void Main(string[] args)
        {

            if (String.IsNullOrEmpty(BatchAccountName) || 
                String.IsNullOrEmpty(BatchAccountKey) ||
                String.IsNullOrEmpty(BatchAccountUrl) ||
                String.IsNullOrEmpty(StorageAccountName) ||
                String.IsNullOrEmpty(StorageAccountKey))
            {
                throw new InvalidOperationException("One or more account credential strings have not been populated. Please ensure that your Batch and Storage account credentials have been specified.");
            }

            try
            {

                Console.WriteLine("Sample start: {0}", DateTime.Now);
                Console.WriteLine();
                Stopwatch timer = new Stopwatch();
                timer.Start();

                // Create the blob client, for use in obtaining references to blob storage containers
                (CloudBlobClient, CloudStorageAccount) cc = CreateCloudBlobClient(StorageAccountName, StorageAccountKey);
                CloudBlobClient blobClient =  cc.Item1;
                CloudStorageAccount linkedStorageAccount = cc.Item2;

                // Use the blob client to create the input container in Azure Storage 
                const string inputContainerName = "input";
                CloudBlobContainer container = blobClient.GetContainerReference(inputContainerName);
                container.CreateIfNotExistsAsync().Wait();

                // The collection of data files that are to be processed by the tasks
                List<string> inputFilePaths = new List<string> {"input.txt"};

                // Upload the data files to Azure Storage. This is the data that will be processed by each of the tasks that are
                // executed on the compute nodes within the pool.                
                UploadFileToContainer(blobClient, inputContainerName, "input.txt");                

                // Get a Batch client using account creds
                BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);

                using (BatchClient batchClient = BatchClient.Open(cred))
                {
                    Console.WriteLine("Creating pool [{0}]...", PoolId);

                    // Create a Windows Server image, VM configuration, Batch pool
                    ImageReference imageReference = CreateImageReference();
                    VirtualMachineConfiguration vmConfiguration = CreateVirtualMachineConfiguration(imageReference);
                    CreateBatchPool(batchClient, vmConfiguration);

                    // Create a Batch job
                    Console.WriteLine("Creating job [{0}]...", JobId);

                    try
                    {
                        CloudJob job = batchClient.JobOperations.CreateJob();
                        job.Id = JobId;
                        job.PoolInformation = new PoolInformation { PoolId = PoolId };
                        job.UsesTaskDependencies = true;

                        // Create the blob storage container for the outputs.                        
                        Task t1 = job.PrepareOutputStorageAsync(linkedStorageAccount);                      
                        Task.WaitAll(t1);

                        string containerName = job.OutputStorageContainerName();                     
                        string containerUrl = job.GetOutputStorageContainerUrl(linkedStorageAccount);
                        job.CommonEnvironmentSettings = new[] { new EnvironmentSetting("JOB_CONTAINER_URL", containerUrl) };

                        job.Commit();
                    }
                    catch (BatchException be)
                    {
                        // Accept the specific error code JobExists as that is expected if the job already exists
                        if (be.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.JobExists)
                        {
                            Console.WriteLine("The job {0} already existed when we tried to create it", JobId);
                        }
                        else
                        {
                            throw; // Any other exception is unexpected
                        }
                    }

                    // Create a collection to hold the tasks that we'll be adding to the job                    
                    List<CloudTask> tasks = new List<CloudTask>();

                    // Create each of the tasks to process one of the input files. 
                    string lastTaskId = null;
                    for (int i = 0; i < 3; i++)
                    {
                        string taskId = String.Format("Task{0}", i);                        
                        CloudTask task;
                        // first task
                        if (lastTaskId == null)
                        {
                            //task = new CloudTask(taskId, $"cmd /c %AZ_BATCH_APP_PACKAGE_TESTBATCHAPP%\\testbatchapp.exe {i}")
                            task = new CloudTask(taskId, $"cmd /c %AZ_BATCH_APP_PACKAGE_HPCAPP%\\testbatchapp.exe {i}")
                                {
                                    ApplicationPackageReferences = new List<ApplicationPackageReference>
                                    {
                                        new ApplicationPackageReference
                                        {
                                            //ApplicationId = "testbatchapp",                                           
                                           ApplicationId = "hpcapp"//,
                                           //Version = "1.1"

                                        }
                                    }
                                };
                        }
                        else
                        {
                            // task = new CloudTask(taskId, $"cmd /c %AZ_BATCH_APP_PACKAGE_TESTBATCHAPP%\\testbatchapp.exe {i}")
                            task = new CloudTask(taskId, $"cmd /c %AZ_BATCH_APP_PACKAGE_HPCAPP%\\testbatchapp.exe {i}")
                            {
                                ApplicationPackageReferences = new List<ApplicationPackageReference>
                                {
                                    new ApplicationPackageReference
                                    {
                                      //  ApplicationId = "testbatchapp",                                       
                                         ApplicationId = "hpcapp"//,
                                         //Version = "1.1"
                                    }
                                }
                            };

                            task.DependsOn = TaskDependencies.OnId(lastTaskId);               
                        }

                        lastTaskId = taskId;
                        tasks.Add(task);
                    }

                    // Add all tasks to the job.
                    batchClient.JobOperations.AddTask(JobId, tasks);


                    // Monitor task success/failure, specifying a maximum amount of time to wait for the tasks to complete.
                    TimeSpan timeout = TimeSpan.FromMinutes(30);
                    Console.WriteLine("Monitoring all tasks for 'Completed' state, timeout in {0}...", timeout);

                    IEnumerable<CloudTask> addedTasks = batchClient.JobOperations.ListTasks(JobId);

                    batchClient.Utilities.CreateTaskStateMonitor().WaitAll(addedTasks, TaskState.Completed, timeout);

                    Console.WriteLine("All tasks reached state Completed.");

                    // Print task output
                    Console.WriteLine();
                    Console.WriteLine("Printing task output...");

                    IEnumerable<CloudTask> completedtasks = batchClient.JobOperations.ListTasks(JobId);
                    foreach (CloudTask task in completedtasks)
                    {
                        List<Task> readTasks = new List<Task>();
                        foreach (OutputFileReference output in task.OutputStorage(linkedStorageAccount).ListOutputs(TaskOutputKind.TaskOutput))
                        {
                           // Console.WriteLine($"output file: {output.FilePath}");
                            readTasks.Add( output.DownloadToFileAsync($"{JobId}-{output.FilePath}", System.IO.FileMode.Create));
                        }

                        Task.WaitAll(readTasks.ToArray());
                    }

                    // Print out some timing info
                    timer.Stop();
                    Console.WriteLine();
                    Console.WriteLine("Sample end: {0}", DateTime.Now);
                    Console.WriteLine("Elapsed time: {0}", timer.Elapsed);

                    // Clean up Storage resources
                    container.DeleteIfExistsAsync().Wait();
                    Console.WriteLine("Container [{0}] deleted.", inputContainerName);

                    // Clean up Batch resources (if the user so chooses)
                    Console.WriteLine();
                    Console.Write("Delete job? [yes] no: ");
                    string response = Console.ReadLine().ToLower();
                    if (response != "n" && response != "no")
                    {
                        batchClient.JobOperations.DeleteJob(JobId);
                    }

                    Console.Write("Delete pool? [yes] no: ");
                    response = Console.ReadLine().ToLower();
                    if (response != "n" && response != "no")
                    {
                        batchClient.PoolOperations.DeletePool(PoolId);
                    }
                }
            }
            finally
            {
                Console.WriteLine();
                Console.WriteLine("Sample complete, hit ENTER to exit...");
                Console.ReadLine();
            }
            
        }

        private static void CreateBatchPool(BatchClient batchClient, VirtualMachineConfiguration vmConfiguration)
        {
            try
            {
                CloudPool pool = batchClient.PoolOperations.CreatePool(
                    poolId: PoolId,
                    targetDedicatedComputeNodes: PoolNodeCount,
                    virtualMachineSize: PoolVMSize,
                    virtualMachineConfiguration: vmConfiguration);

                // Specify the application and version to install on the compute nodes
                pool.ApplicationPackageReferences = new List<ApplicationPackageReference>
                    {
                        new ApplicationPackageReference {
                            //ApplicationId = "testbatchapp",
                            ApplicationId = "hpcapp"//,
                            //Version = "1.1" 
                        }
                    };

                pool.Commit();
            }
            catch (BatchException be)
            {
                // Accept the specific error code PoolExists as that is expected if the pool already exists
                if (be.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.PoolExists)
                {
                    Console.WriteLine("The pool {0} already existed when we tried to create it", PoolId);
                }
                else
                {
                    throw; // Any other exception is unexpected
                }
            }
        }

        private static VirtualMachineConfiguration CreateVirtualMachineConfiguration(ImageReference imageReference)
        {
            return new VirtualMachineConfiguration(
                imageReference: imageReference,
                nodeAgentSkuId: "batch.node.windows amd64");
        }

        private static ImageReference CreateImageReference()
        {
            return new ImageReference(
                publisher: "MicrosoftWindowsServer",
                offer: "WindowsServer",
                sku: "2016-datacenter-smalldisk",
                version: "latest");
        }

        /// <summary>
        /// Creates a blob client
        /// </summary>
        /// <param name="storageAccountName">The name of the Storage Account</param>
        /// <param name="storageAccountKey">The key of the Storage Account</param>
        /// <returns></returns>
        private static (CloudBlobClient, CloudStorageAccount) CreateCloudBlobClient(string storageAccountName, string storageAccountKey)
        {
            // Construct the Storage account connection string
            string storageConnectionString =
                $"DefaultEndpointsProtocol=https;AccountName={storageAccountName};AccountKey={storageAccountKey}";

            // Retrieve the storage account
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the blob client
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();          
            return (blobClient, storageAccount);
        }


        /// <summary>
        /// UPLOADS the specified file to the specified Blob container, so the task can consume it via task.ResourceFiles
        /// </summary>
        /// <param name="blobClient">A <see cref="CloudBlobClient"/>.</param>
        /// <param name="containerName">The name of the blob storage container to which the file should be uploaded.</param>
        /// <param name="filePath">The full path to the file to upload to Storage.</param>
        /// <returns>A ResourceFile instance representing the file within blob storage.</returns>
        private static ResourceFile UploadFileToContainer(CloudBlobClient blobClient, string containerName, string filePath)
        {
            Console.WriteLine("Uploading file {0} to container [{1}]...", filePath, containerName);

            string blobName = Path.GetFileName(filePath);

            filePath = Path.Combine(Environment.CurrentDirectory, filePath);

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            CloudBlockBlob blobData = container.GetBlockBlobReference(blobName);
            blobData.UploadFromFileAsync(filePath).Wait();

            // Set the expiry time and permissions for the blob shared access signature. 
            // In this case, no start time is specified, so the shared access signature 
            // becomes valid immediately
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Read
            };

            // Construct the SAS URL for blob
            string sasBlobToken = blobData.GetSharedAccessSignature(sasConstraints);
            string blobSasUri = String.Format("{0}{1}", blobData.Uri, sasBlobToken);
            // other example uses bookFileUri  = blobData.Uri.ToString() as arg0 for topNWords
            // which then makes CloudBlockBlob blob = new CloudBlockBlob(new Uri(arg0), storageCred) to recover stuff FROM THE EXE
            // arg0 is "https://emergentai.blob.core.windows.net/input/input.txt"

            return ResourceFile.FromUrl(blobSasUri, filePath);
        }

    }
}
