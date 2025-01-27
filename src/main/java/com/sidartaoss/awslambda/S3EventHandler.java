package com.sidartaoss.awslambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;

public class S3EventHandler implements RequestHandler<S3Event, String> {

    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private final AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String queueUrl =
            "https://sqs.us-east-1.amazonaws.com/337909750645/teste-sqs";

    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        s3Event.getRecords().forEach(record -> {
            String bucketName = record.getS3().getBucket().getName();
            String key = record.getS3().getObject().getKey();

            try (S3Object s3Object = s3Client.getObject(bucketName, key);
                 InputStream inputStream = s3Object.getObjectContent()) {

                final var data = objectMapper.readValue(inputStream, Object.class);
                context.getLogger().log("Sucesso: " + data.toString());

                SendMessageRequest sendMsgRequest = new SendMessageRequest()
                        .withQueueUrl(queueUrl)
                        .withMessageBody(objectMapper.writeValueAsString(data));
                sqsClient.sendMessage(sendMsgRequest);

            } catch (Exception e) {
                context.getLogger().log("Erro ao ler o JSON do S3: " + e.getMessage());
            }
        });
        return "Processamento concluído";
    }
}
