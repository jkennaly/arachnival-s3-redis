
const aws = require('aws-sdk');
const redis = require("redis")
const client = redis.createClient({url: process.env.REDIS_URL})

const s3 = new aws.S3();


exports.handler = async (event, context) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));

    // Get the object from the event and show its content type
    const bucket = event.Records[0].s3.bucket.name;
    const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    let result;
    let body;
    try {
        const params = {
            Bucket: bucket,
            Key: key
        };
        
        result = await s3.getObject(params).promise();
        if(!result || !result.Body || ! result.Body.toString('utf-8')) throw new Error('invalid S3 object')
	    
    } catch (error) {
        console.log(error);
        return;
    }
    try {
    	body = result.Body.toString('utf-8')
    	if(body === 's3AccessCheck') return;
    } catch (error) {
        console.error('Data parse error');
        console.error(event.Records[0]);
        console.error(body);
        console.error(error);
        return;
    }
    try {
        const full = JSON.parse(body)
    } catch (error) {
        console.error('JSON parsing error');
        console.error(event.Records[0]);
        console.error(body);
        console.error(error);
        return;
    }

    try {
		const lineup = full.Blocks
			.filter(b => b.BlockType === 'LINE')
			.map(b => b.Text)
	    const leKey = 'arach-lineup.' + key
	    await client.connect()
	  	await client.set(leKey, lineup, {
			EX: 3600 * 24 * 30
		})
	    await client.quit()
    } catch (error) {
        console.error('Data handling error');
        console.error(event.Records[0]);
        console.error(body);
        console.error(error);
        return;
    }
};
