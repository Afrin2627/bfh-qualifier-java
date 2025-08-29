package com.example.bfh.service;

import com.example.bfh.model.GenerateWebhookRequest;
import com.example.bfh.model.GenerateWebhookResponse;
import com.example.bfh.model.FinalQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;

@Component
public class QualifierRunner implements ApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(QualifierRunner.class);

  private final WebClient webClient;
  private final String generatePath;
  private final String submitFallbackPath;
  private final String name;
  private final String regNo;
  private final String email;
  private final Resource q1;
  private final Resource q2;
  private final String outFileName;

  public QualifierRunner(WebClient webClient,
                         @Value("${app.endpoints.generate}") String generatePath,
                         @Value("${app.endpoints.submitFallback}") String submitFallbackPath,
                         @Value("${app.candidate.name}") String name,
                         @Value("${app.candidate.regNo}") String regNo,
                         @Value("${app.candidate.email}") String email,
                         @Value("${app.sql.q1}") Resource q1,
                         @Value("${app.sql.q2}") Resource q2,
                         @Value("${app.output.storeFile}") String outFileName) {
    this.webClient = webClient;
    this.generatePath = generatePath;
    this.submitFallbackPath = submitFallbackPath;
    this.name = name; this.regNo = regNo; this.email = email;
    this.q1 = q1; this.q2 = q2; this.outFileName = outFileName;
  }

  @Override
  public void run(ApplicationArguments args) throws Exception {
    log.info("Starting qualifier flow...");

    GenerateWebhookRequest payload = new GenerateWebhookRequest(name, regNo, email);

    GenerateWebhookResponse gw = webClient.post()
        .uri(generatePath)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(payload)
        .retrieve()
        .bodyToMono(GenerateWebhookResponse.class)
        .doOnNext(r -> log.info("Received webhook: {}", r.getWebhook()))
        .block();

    if (gw == null) throw new IllegalStateException("Null response");

    String sql = chooseSqlByRegNo(regNo);
    storeToFile(sql, outFileName);

    FinalQueryRequest finalBody = new FinalQueryRequest(sql);

    String submitUrl = gw.getWebhook() != null && !gw.getWebhook().isBlank() ? gw.getWebhook() : submitFallbackPath;

    log.info("Submitting final query to: {}", submitUrl);

    String token = gw.getAccessToken();

    Mono<String> submit = webClient.post()
        .uri(submitUrl)
        .contentType(MediaType.APPLICATION_JSON)
        .headers(h -> {
          if (token != null && !token.isBlank()) {
            h.set(HttpHeaders.AUTHORIZATION, token);
          }
        })
        .bodyValue(finalBody)
        .retrieve()
        .bodyToMono(String.class)
        .doOnError(err -> log.error("Submission failed: {}", err.toString()));

    String response = submit.block();
    log.info("Submission response: {}", response);

    log.info("Flow complete.");
  }

  private String chooseSqlByRegNo(String regNo) throws Exception {
    String digitsOnly = regNo.replaceAll("\\D", "");
    int lastTwo = Integer.parseInt(digitsOnly.substring(Math.max(0, digitsOnly.length()-2)));
    boolean isOdd = (lastTwo % 2) == 1;
    return isOdd ? readResource(q1) : readResource(q2);
  }

  private String readResource(Resource res) throws Exception {
    byte[] bytes = FileCopyUtils.copyToByteArray(res.getInputStream());
    return new String(bytes, StandardCharsets.UTF_8).trim();
  }

  private void storeToFile(String sql, String fileName) throws Exception {
    File f = new File(fileName);
    try (FileOutputStream fos = new FileOutputStream(f)) {
      fos.write(sql.getBytes(StandardCharsets.UTF_8));
    }
  }
}
