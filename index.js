
const aws = require('aws-sdk');
const redis = require("redis")
const client = redis.createClient({url: process.env.REDIS_URL})

const textract = new aws.Textract();


exports.handler = async (event, context) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));

    // Get the object from the event and show its content type
    const bucket = event.Records[0].s3.bucket.name;
    const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    let result;
    try {
        const params = {
            Bucket: bucket,
            Key: key
        };
        
        result = await s3.getObject(params).promise();
        if(!result || !result.Body || ! result.Body.toString('utf-8')) throw new Error('invalid S3 object')
	    const full = JSON.parse(result)
		const lineup = full.Blocks
			.filter(b => b.BlockType === 'LINE')
			.map(b => b.Text)
	    const leKey = 'arach-lineup.' + key
	    await client.connect()
	  	await client.set(leKey, lineup, {
			EX: 3600 * 24 * 30
		})
	    client.quit()
    } catch (error) {
        console.log(error);
        return;
    }

    return job
};
