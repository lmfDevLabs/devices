const { PubSub } = require('@google-cloud/pubsub');
const fs = require('fs');
const csvParser = require('csv-parser');
const fetch = require('node-fetch');

// global vars
const topicName = 'projects/sensebuy-e8add/topics/telemetry'; // Reemplaza YOUR_PROJECT_ID con tu ID de proyecto
const apiUrl = 'https://us-central1-sensebuy-e8add.cloudfunctions.net/api/v1/buyersToken/lsmkexTISq5JzCmIMUi5'; // Endpoint de tu API para obtener credenciales
const bearerToken = 'eyJhbGciOiJSUzI1NiIsImtpZCI6IjVkNjE3N2E5Mjg2ZDI1Njg0NTI2OWEzMTM2ZDNmNjY0MjZhNGQ2NDIiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL3NlY3VyZXRva2VuLmdvb2dsZS5jb20vc2Vuc2VidXktZThhZGQiLCJhdWQiOiJzZW5zZWJ1eS1lOGFkZCIsImF1dGhfdGltZSI6MTcxNzIwMDQxOCwidXNlcl9pZCI6InFZdmkxVzRLRnpmSXFWT2hhOEpDdDJYb05GNDIiLCJzdWIiOiJxWXZpMVc0S0Z6ZklxVk9oYThKQ3QyWG9ORjQyIiwiaWF0IjoxNzE3MjAwNDE4LCJleHAiOjE3MTcyMDQwMTgsImVtYWlsIjoiYnV5ZXJvbmVAbWUuY28iLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsImZpcmViYXNlIjp7ImlkZW50aXRpZXMiOnsiZW1haWwiOlsiYnV5ZXJvbmVAbWUuY28iXX0sInNpZ25faW5fcHJvdmlkZXIiOiJwYXNzd29yZCJ9fQ.lDtb4fkYMqdegWCNByvPmspK2yCKQvoDhhzOKz4K3xxNg07tX3759np9XrWLiwGSpGIkiDIHcDMeJUGcTEEFANOKS7gbcAtj6BwPDTRvlyXWOyYjjLWz-WvKo5nlt7ErBZEDYNuwU6T5Omn_eymX_Edqg77eprsqFMznuAX-zE-m0emB5acPn07ba_9X6lP0D9pyokPfJeWhjGEavOsFB8Pi57s9JW138WOXhnFOU3S8WnfAQtjJeyFhO6b95MrMxdGMLS9mehQ2mzXW7aoXU9D_gFWLal010NHS1LwbJDcvSNaKeQa0Is5xvgNjvBdO4typCbtkdDwOcg5ZHZ_qHg'
const buyerId = 'jigt0CRWVs49cArvRFPS';

// Leer coordenadas desde el archivo CSV
const filepath = './coordsH1.csv';
let coordsData = [];

// read coords file
fs.createReadStream(filepath)
  .pipe(csvParser())
  .on('data', (row) => {
    coordsData.push(row);
  })
  .on('end', () => {
    // Obtener las credenciales antes de publicar las coordenadas
    getCredentialsAndPublishCoords();
  });

async function getCredentialsAndPublishCoords() {
  // get credentials for send messages to pubsub topic
  try {
    // Hacer la petición para obtener las credenciales
    const response = await fetch(apiUrl, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${bearerToken}`,
        'Content-Type': 'application/json'
      }
    });

    // check for response
    if (!response.ok) {
      throw new Error('Failed to fetch credentials');
    }

    // pubsub credentials
    // Escribir las credenciales en un archivo temporal
    const credentials = await response.json();
    console.log({credentials})
    const tempCredentialsPath = './temp-credentials.json';
    fs.writeFileSync(tempCredentialsPath, JSON.stringify(credentials));
    // Configurar la variable de entorno para usar las credenciales descargadas
    process.env.GOOGLE_APPLICATION_CREDENTIALS = tempCredentialsPath;

    // Ahora podemos inicializar PubSub y publicar las coordenadas
    const pubsub = new PubSub();

    for (let i = 0; i < coordsData.length; i++) {
      setTimeout(async () => {
        const data = JSON.stringify({
          buyerId,
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
            active:true,
            connectionStatus:10,
            batteryLife:100
          }
        });

        // data buffer
        const dataBuffer = Buffer.from(data);

        // send messages to pubsub
        try {
          console.log(`Message ${data} published.`);
          const messageId = await pubsub.topic(topicName).publish(dataBuffer);
          console.log(`Message ${messageId} published.`);
        } catch (error) {
          console.error(`Received error while publishing: ${error.message}`);
        }
      }, i * 1000); // Esperar 1 segundo entre mensajes
    }
  } catch (error) {
    console.error('Error fetching credentials:', error);
  }
}





