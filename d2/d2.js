const { PubSub } = require('@google-cloud/pubsub');
const fs = require('fs');
const csvParser = require('csv-parser');
const pubsub = new PubSub();
// global vars
const topicName = 'projects/sensebuy-e8add/topics/telemetry'
const buyerId = 'aU8nMP5E2F80SZJEx1rb';
process.env.GOOGLE_APPLICATION_CREDENTIALS = '/Users/carlosalbertotalerojacome/Documents/dev/SB/devices/d1/sensebuy-e8add-482dddf1f0e3.json';

// Leer coordenadas desde el archivo CSV
const filepath = './coordsH2.csv';
let coordsData = [];

// read coords external data
fs.createReadStream(filepath)
  .pipe(csvParser())
  .on('data', (row) => {
    coordsData.push(row);
		console.log(row)
  })
  .on('end', () => {
    publishDataFromMobileDevice();
  });

// to publish data
const publishDataFromMobileDevice = async () => {
  for (let i = 0; i < coordsData.length; i++) {
    setTimeout(async () => {
      const data = JSON.stringify({
				buyerId: buyerId,
				dataMobilDevice:{
					lastMessageReceived:new Date().toISOString(),
					liveCoords: {
						hash: '',
						lat: parseFloat(coordsData[i].latitude),
						lng: parseFloat(coordsData[i].longitude),
						nameOfPoint: coordsData[i].nameOfPoint
					},
					liveFeed: {
						matchQuality: {
							color: '',
							value:{r:0,g:0,b:0}
						},
						matrixDistance: {
							color: '',
							value:{r:0,g:0,b:0}
						},
						motorSpeed:0,
					},
				},
				statusOfBracelet:{
					active:false,
					connectionStatus:0,
					batteryLife:0
				}
			});

			// data buffer
      const dataBuffer = Buffer.from(data);
			// send message
      try {
				console.log(`Message ${data} published.`);
        const messageId = await pubsub.topic(topicName).publish(dataBuffer);
        console.log(`Message ${messageId} published.`);
      } catch (error) {
        console.error(`Received error while publishing: ${error.message}`);
      }
    }, i * 1000); // Esperar 1 segundo entre mensajes
  }
}










