
const aws = require('aws-sdk');
const redis = require("redis")
const client = redis.createClient({url: process.env.REDIS_URL})

const s3 = new aws.S3();


exports.handler = async (event, context) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));

    // Get the object from the event and show its content type
    const bucket = event.Records[0].s3.bucket.name;
    const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    const matched = key.match(/^\w+\/(\w+)\/.*/)
    if(!matched || !matched.length) {
    	console.error('invalid key')
    	console.error(key)
    	console.error(JSON.stringify(event, null, 2))
    }
    const job = matched[1]
    let result, full, body
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
    	if(body === 's3AccessCheck') {
    		console.log('s3AccessCheck not processed')
    		return	
    	} 
    } catch (error) {
        console.error('Data parse error');
        console.error(event.Records[0]);
        console.error(body);
        console.error(error);
        return;
    }
    try {
        full = JSON.parse(body)
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
	    await client.connect()
	    const rawIndex = `textract-job.${job}`
    	//console.log('rawIndex', rawIndex, lineup)
	    const leRaw = await client.get(rawIndex)
	    if(!leRaw) {
        	console.error('No lineupUrl found')
        	console.error(rawIndex)
        	console.error(leRaw)
	    } else {
		    const leIndex = decodeURIComponent(leRaw.replace(/\+/g, ' '))
		    	.match(/(.*)\..+$/)[1]
	    	const leKey = 'arach-lineup.' + leIndex
    		console.log('leIndex', leIndex, leKey)
		  	await client.set(leKey, JSON.stringify(lineup), {
				EX: 3600 * 24 * 30
			})
		}
    } catch (error) {
        console.error('Data handling error');
        console.error(event.Records[0]);
        console.error(body);
        console.error(error);
        return;
    } finally {
    	await client.quit()
    }
};
