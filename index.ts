import {
    AuthApi,
    Token,
    JobMediatorInput,
    Locator,
    VideoAnalysisInput,
    Job,
    MediatorJob,
    JobsApi,
    JobMediatorStatus,
    JobsApiApiKeys,
	SpeechToTextOutput,
	SpeechToTextInput
} from 'cortex-client'

import fs, {ReadStream} from 'fs'
import * as AWS from 'aws-sdk';


const appConfigFile: string = <string> process.env.APP_CONFIG_FILE;
const appConfig = JSON.parse(fs.readFileSync(appConfigFile).toString());

const client: string = appConfig['clientKey'];
const secret: string = appConfig['clientSecret'];

const projectServiceId: string = appConfig['projectServiceId'];
const aws_access_key_id: string = appConfig['aws_access_key_id'];
const aws_secret_access_key: string = appConfig['aws_secret_access_key'];
const aws_session_token: string = appConfig['aws_session_token'];

AWS.config.update({accessKeyId: aws_access_key_id, secretAccessKey: aws_secret_access_key});
AWS.config.region = appConfig['bucketRegion'];

const credentials = new AWS.Credentials(aws_access_key_id, aws_secret_access_key, aws_session_token);
const s3 = new AWS.S3({
    apiVersion: '2006-03-01',
	signatureVersion: 'v4',
    s3ForcePathStyle: true,
	credentials: credentials
});

const inputMediaFile: {
    folder: string,
    name: string,
    duration: number
} = {
    "folder": appConfig['localPath'],
    "name": appConfig['inputFile'],
    "duration": 30
};
const s3Bucket: string = appConfig['bucketName'];
const outputFileName_json: string = appConfig['outputFile_json'];
const outputFileName_ttml: string = appConfig['outputFile_ttml'];
const outputFileName_text: string = appConfig['outputFile_text'];


const baseUrl: string = appConfig['host'];
const authApi: AuthApi = new AuthApi(baseUrl);
const jobsApi: JobsApi = new JobsApi(baseUrl);


const createMediatorSTJobInput = (s3InputSignedUrl: string, s3OutputSignedUrl_json: string, s3OutputSignedUrl_ttml: string, s3OutputSignedUrl_text: string ): JobMediatorInput => {

    let inputFile: Locator  = new Locator();
    inputFile.awsS3Bucket   = s3Bucket;
    inputFile.awsS3Key      = inputMediaFile.name;
    inputFile.httpEndpoint  = s3InputSignedUrl;
	
    let jsonFormat: Locator  = new Locator();
    jsonFormat.awsS3Bucket   = s3Bucket;
    jsonFormat.awsS3Key      = outputFileName_json;
    jsonFormat.httpEndpoint  = s3OutputSignedUrl_json;
	
    let ttmlFormat: Locator  = new Locator();
    ttmlFormat.awsS3Bucket   = s3Bucket;
    ttmlFormat.awsS3Key      = outputFileName_ttml;
    ttmlFormat.httpEndpoint  = s3OutputSignedUrl_ttml;
	
    let textFormat: Locator  = new Locator();
    textFormat.awsS3Bucket   = s3Bucket;
    textFormat.awsS3Key      = outputFileName_text;
    textFormat.httpEndpoint  = s3OutputSignedUrl_text;
	
    let outputLocation: SpeechToTextOutput  = new SpeechToTextOutput();
	outputLocation.jsonFormat = jsonFormat;
	outputLocation.ttmlFormat = ttmlFormat;
	outputLocation.textFormat = textFormat;
			
    let jobInput: SpeechToTextInput = new SpeechToTextInput();
    jobInput.jobInputType   = SpeechToTextInput.name;
    jobInput.inputFile      = inputFile;
    jobInput.outputLocation = outputLocation;

    let job: Job  = new Job();
    job.jobType     = Job.JobTypeEnum.AiJob;
    job.jobProfile  = Job.JobProfileEnum.MediaCortexSpeechToText;
    job.jobInput    = jobInput;

    let jobMediatorInput: JobMediatorInput  = new JobMediatorInput();
    jobMediatorInput.projectServiceId   = projectServiceId;
    jobMediatorInput.quantity           = inputMediaFile.duration;
    jobMediatorInput.job                = job;

    return jobMediatorInput;
};



const getAccessToken = async (): Promise<Token> => {
    return (await authApi.getAccessToken(client, secret)).body;
};

const submitJob = async (jobInput: JobMediatorInput): Promise<MediatorJob> => {
    return (await jobsApi.createJob(jobInput)).body;
};

const waitForComplete = async (job: MediatorJob): Promise<MediatorJob> => {
    const delay = (ms: number) => {
        return new Promise( resolve => setTimeout(resolve, ms));
    };
    let mediatorStatus: JobMediatorStatus = job.status;
    while (mediatorStatus.status !== JobMediatorStatus.StatusEnum.Completed
        && mediatorStatus.status !== JobMediatorStatus.StatusEnum.Failed) {
        await delay(30000);
        job = (await jobsApi.getJobById(job.id)).body;
        mediatorStatus = job.status;
        console.log(mediatorStatus);
    }
    return job;
};

const uploadMedia = async () => {
    const read: ReadStream = fs.createReadStream(inputMediaFile.folder + inputMediaFile.name);
    const params: AWS.S3.PutObjectRequest = {
        Bucket: s3Bucket,
        Key: inputMediaFile.name,
        Body: read
    };
    await s3.upload(params).promise();
};

const downloadResult_json = async () => {
    console.log('starting download json result process ...');
    const params: AWS.S3.GetObjectRequest = {
        Bucket: s3Bucket,
        Key: outputFileName_json
    };
    const result = await s3.getObject(params).promise();
    fs.writeFileSync(inputMediaFile.folder + outputFileName_json, result.Body);
};

const downloadResult_ttml = async () => {
    console.log('starting download ttml result process ...');
    const params: AWS.S3.GetObjectRequest = {
        Bucket: s3Bucket,
        Key: outputFileName_ttml
    };
    const result = await s3.getObject(params).promise();
    fs.writeFileSync(inputMediaFile.folder + outputFileName_ttml, result.Body);
};

const downloadResult_text = async () => {
    console.log('starting download text result process ...');
    const params: AWS.S3.GetObjectRequest = {
        Bucket: s3Bucket,
        Key: outputFileName_text
    };
    const result = await s3.getObject(params).promise();
    fs.writeFileSync(inputMediaFile.folder + outputFileName_text, result.Body);
};


const deleteArtifacts = async () => {
    console.log('Deleting artifacts from S3 ...');
    const inputParams: AWS.S3.DeleteObjectRequest = {
        Bucket: s3Bucket,
        Key: inputMediaFile.name
    };
    const outputParams_json: AWS.S3.DeleteObjectRequest = {
        Bucket: s3Bucket,
        Key: outputFileName_json
    };
    const outputParams_ttml: AWS.S3.DeleteObjectRequest = {
        Bucket: s3Bucket,
        Key: outputFileName_ttml
    };
    const outputParams_text: AWS.S3.DeleteObjectRequest = {
        Bucket: s3Bucket,
        Key: outputFileName_text
    };
	
    await Promise.all([
        s3.deleteObject(inputParams).promise(),
        s3.deleteObject(outputParams_json).promise(),
		s3.deleteObject(outputParams_ttml).promise(),
		s3.deleteObject(outputParams_text).promise()
    ]);
};

const generateGetSignedUrl = () : string => {
    return s3.getSignedUrl('getObject', {
        "Bucket": s3Bucket,
        "Key": inputMediaFile.name
    });
};

const generatePutSignedUrl_json = () : string => {
    return s3.getSignedUrl('putObject', {
        "Bucket": s3Bucket,
        "Key": outputFileName_json,
        "ContentType": "application/json",
        "Expires": 60 * 60 * 1
    });
};

const generatePutSignedUrl_ttml = () : string => {
    return s3.getSignedUrl('putObject', {
        "Bucket": s3Bucket,
        "Key": outputFileName_ttml,
        "Expires": 60 * 60 * 1
    });
};

const generatePutSignedUrl_text = () : string => {
    return s3.getSignedUrl('putObject', {
        "Bucket": s3Bucket,
        "Key": outputFileName_text,
        "Expires": 60 * 60 * 1
    });
};



const main = async () => {
    try {
        console.log("Starting AI process ..");

        // Upload media to S3.
        await uploadMedia();
        console.log('media uploaded to s3 successfully');

        // Generate signed URL for input and output.
        const s3InputSignedUrl: string = generateGetSignedUrl();
        console.log(`Input file signed URL: ${s3InputSignedUrl}`);

        const s3OutputSignedUrl_json: string = generatePutSignedUrl_json();
        console.log(`Output json file signed URL: ${s3OutputSignedUrl_json}`);

        const s3OutputSignedUrl_ttml: string = generatePutSignedUrl_ttml();
        console.log(`Output ttml file signed URL: ${s3OutputSignedUrl_ttml}`);

        const s3OutputSignedUrl_text: string = generatePutSignedUrl_text();
        console.log(`Output text file signed URL: ${s3OutputSignedUrl_text}`);
		
				
        // Create Video Indexer job model.
        const mediatorJobInput: JobMediatorInput = createMediatorSTJobInput(s3InputSignedUrl, s3OutputSignedUrl_json, s3OutputSignedUrl_ttml, s3OutputSignedUrl_text);
        console.log(JSON.stringify(mediatorJobInput));

        // Get API access-token for the this client.
        const token: Token = await getAccessToken();
        console.log(token);

        // Update jobs API with the generated access token
        jobsApi.setApiKey(JobsApiApiKeys.tokenSignature, token.authorization);

        // Submit the job to Mediator.
        const submittedJob: MediatorJob = await submitJob(mediatorJobInput);
        console.log(submittedJob);

        // Wait until job is done and get result.
        const completedJob = await waitForComplete(submittedJob);
        console.log(completedJob);

        // Download result.
        await downloadResult_json();
        console.log('result json file downloaded successfully');

        await downloadResult_ttml();
        console.log('result ttml file downloaded successfully');
		
        await downloadResult_text();
        console.log('result text file downloaded successfully');
				
        // delete artifacts from s3
        await deleteArtifacts();
        console.log('Deleted all artifacts from S3');

        console.log("AI process has finished successfully");

    } catch (e) {
        console.log(e);
    }
};

main().then();
