package io.jonasg.kawa.server.auth;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.KafkaFrameDecoder;
import io.jonasg.kawa.protocol.kafka.KafkaFrameEncoder;
import io.jonasg.kawa.protocol.kafka.KafkaHeader;
import io.jonasg.kawa.protocol.kafka.ResponseHeaderCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class BrokerSaslIamAuthenticatorTest {

    private static final String HOST = "b-1.gateway.abc123.c2.kafka.eu-west-1.amazonaws.com";
    private static final String ACCEPTED = "{\"version\":\"2020_10_22\",\"request-id\":\"req-1\"}";
    private static final String PROFILE_KEY = "AKIAPROFILEDEV00001";
    private static final String PROFILE_SECRET = "profileSecretNeverLogged00001";
    private static final String PROFILE_SESSION = "profileSessionNeverLogged00001";
    private static final String DEFAULT_KEY = "AKIADEFAULTCHAIN00001";
    private static final String DEFAULT_SECRET = "defaultSecretNeverLogged00001";
    private static final List<String> AWS_PROPERTIES =
            List.of("aws.sharedCredentialsFile", "aws.accessKeyId", "aws.secretAccessKey", "aws.sessionToken");
    private static final Pattern CREDENTIAL = Pattern.compile("\"x-amz-credential\":\"([^\"]+)\"");

    @TempDir
    Path dir;

    private final KafkaBodyCodec codec = new KafkaBodyCodec(KafkaApiRegistry.create());
    private final ResponseHeaderCodec responseHeaderCodec = new ResponseHeaderCodec();
    private final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();
    private final Map<String, String> savedProperties = new HashMap<>();

    @BeforeEach
    void captureLogsAndIsolateAwsSettings() throws IOException {
        logs.start();
        root.addAppender(logs);
        for (String key : AWS_PROPERTIES) {
            savedProperties.put(key, System.getProperty(key));
            System.clearProperty(key);
        }
        useCredentialsFile("");
    }

    @AfterEach
    void restoreLogsAndAwsSettings() {
        root.detachAppender(logs);
        logs.stop();
        for (String key : AWS_PROPERTIES) {
            String value = savedProperties.get(key);
            if (value == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, value);
            }
        }
    }

    @Test
    void asksTheBrokerToSignInWithAwsMskIam() throws IOException {
        // given a broker that offers AWS_MSK_IAM and a gateway configured for it
        useProfileFile();
        var channel = brokerChannel(handshakeOffering("AWS_MSK_IAM"), accepted());

        // when
        signIn(iam(null, "dev"), channel);

        // then the handshake asks for the AWS_MSK_IAM mechanism
        String handshake = readFrame(channel);
        assertThat(handshake)
                .withFailMessage(() -> "The gateway did not ask the broker to sign in with AWS_MSK_IAM")
                .contains("AWS_MSK_IAM");
    }

    @Test
    void signsInWithTheConfiguredRegion() throws IOException {
        // given a profile and a configured region that differs from the broker host
        useProfileFile();
        var channel = brokerChannel(handshakeOffering("AWS_MSK_IAM"), accepted());

        // when
        signIn(iam("us-east-2", "dev"), channel);

        // then the request is signed for the configured region
        String authenticate = readAuthenticateFrame(channel);
        assertThat(signedCredential(authenticate))
                .withFailMessage(() -> "The sign-in was not signed for the configured region us-east-2: " + signedCredential(authenticate))
                .contains("/us-east-2/kafka-cluster/");
    }

    @Test
    void derivesTheRegionFromTheBrokerHostnameWhenNoRegionIsSet() throws IOException {
        // given a profile and no configured region
        useProfileFile();
        var channel = brokerChannel(handshakeOffering("AWS_MSK_IAM"), accepted());

        // when
        signIn(iam(null, "dev"), channel);

        // then the request is signed for the region named in the broker hostname
        String authenticate = readAuthenticateFrame(channel);
        assertThat(signedCredential(authenticate))
                .withFailMessage(() -> "The region was not taken from the broker hostname eu-west-1: " + signedCredential(authenticate))
                .contains("/eu-west-1/kafka-cluster/");
    }

    @Test
    void signsInWithTheCredentialsOfTheConfiguredProfile() throws IOException {
        // given a profile and default-chain credentials that differ from it
        useProfileFile();
        System.setProperty("aws.accessKeyId", DEFAULT_KEY);
        System.setProperty("aws.secretAccessKey", DEFAULT_SECRET);
        var channel = brokerChannel(handshakeOffering("AWS_MSK_IAM"), accepted());

        // when
        signIn(iam(null, "dev"), channel);

        // then the request is signed with the profile's access key
        String authenticate = readAuthenticateFrame(channel);
        assertThat(authenticate)
                .withFailMessage(() -> "The sign-in did not use the access key of profile 'dev'")
                .contains(PROFILE_KEY)
                .doesNotContain(DEFAULT_KEY);
    }

    @Test
    void signsInWithTheDefaultCredentialChainWhenNoProfileIsSet() {
        // given default-chain credentials and no profile
        System.setProperty("aws.accessKeyId", DEFAULT_KEY);
        System.setProperty("aws.secretAccessKey", DEFAULT_SECRET);
        var channel = brokerChannel(handshakeOffering("AWS_MSK_IAM"), accepted());

        // when
        signIn(iam(null, null), channel);

        // then the request is signed with the default chain's access key
        String authenticate = readAuthenticateFrame(channel);
        assertThat(authenticate)
                .withFailMessage(() -> "The sign-in did not use the default AWS credential chain")
                .contains(DEFAULT_KEY);
    }

    @Test
    void authenticatesWhenTheBrokerAcceptsTheSignedRequest() throws IOException {
        // given a profile and a broker that accepts the signed request
        useProfileFile();
        var channel = brokerChannel(handshakeOffering("AWS_MSK_IAM"), accepted());

        // when
        boolean success = signIn(iam(null, "dev"), channel);

        // then the gateway is signed in to the broker
        assertThat(success)
                .withFailMessage(() -> "The broker accepted the AWS_MSK_IAM sign-in but the gateway did not authenticate")
                .isTrue();
    }

    @Test
    void rejectsAndNamesAwsMskIamWhenTheBrokerDeniesTheSignIn() throws IOException {
        // given a profile and a broker that denies the signed request
        useProfileFile();
        var channel = brokerChannel(handshakeOffering("AWS_MSK_IAM"), denied());

        // when
        boolean success = signIn(iam(null, "dev"), channel);

        // then the sign-in is refused and the log names AWS_MSK_IAM
        assertThat(success)
                .withFailMessage(() -> "The gateway authenticated although the broker denied the AWS_MSK_IAM sign-in")
                .isFalse();
        assertThat(logText())
                .withFailMessage(() -> "The refused AWS_MSK_IAM sign-in was not logged with a clear error naming AWS_MSK_IAM:\n" + logText())
                .contains("AWS_MSK_IAM");
    }

    @Test
    void logsAClearErrorNamingAwsMskIamWhenTheProfileHasNoCredentials() throws IOException {
        // given a profile that the credentials file does not define
        useProfileFile();
        var channel = brokerChannel(handshakeOffering("AWS_MSK_IAM"), accepted());

        // when
        boolean success = signIn(iam(null, "missing"), channel);

        // then the gateway does not sign in and the log names AWS_MSK_IAM
        assertThat(success)
                .withFailMessage(() -> "The gateway signed in without any AWS credentials for profile 'missing'")
                .isFalse();
        assertThat(logText())
                .withFailMessage(() -> "The missing AWS credentials were not logged with a clear error naming AWS_MSK_IAM:\n" + logText())
                .contains("AWS_MSK_IAM");
    }

    @Test
    void neverLogsAwsSecretsWhenTheSignInSucceeds() throws IOException {
        // given a profile with a secret and session token, and a broker that accepts the sign-in
        useProfileFile();
        var channel = brokerChannel(handshakeOffering("AWS_MSK_IAM"), accepted());

        // when
        signIn(iam(null, "dev"), channel);

        // then the signed request was built and no AWS secret reached the log
        assertThat(readAuthenticateFrame(channel))
                .withFailMessage(() -> "The sign-in did not build an AWS_MSK_IAM request")
                .contains("x-amz-credential");
        assertNoAwsSecretsInLogs();
    }

    @Test
    void neverLogsAwsSecretsWhenTheBrokerDeniesTheSignIn() throws IOException {
        // given a profile with a secret and session token, and a broker that denies the sign-in
        useProfileFile();
        var channel = brokerChannel(handshakeOffering("AWS_MSK_IAM"), denied());

        // when
        signIn(iam(null, "dev"), channel);

        // then the refusal was logged and no AWS secret reached the log
        assertThat(logText())
                .withFailMessage(() -> "The refused AWS_MSK_IAM sign-in was not logged:\n" + logText())
                .contains("AWS_MSK_IAM");
        assertNoAwsSecretsInLogs();
    }

    private void assertNoAwsSecretsInLogs() {
        String logged = logText();
        assertThat(logged)
                .withFailMessage(() -> "An AWS access key appeared in the gateway log")
                .doesNotContain(PROFILE_KEY);
        assertThat(logged)
                .withFailMessage(() -> "An AWS secret access key appeared in the gateway log")
                .doesNotContain(PROFILE_SECRET);
        assertThat(logged)
                .withFailMessage(() -> "An AWS session token appeared in the gateway log")
                .doesNotContain(PROFILE_SESSION);
    }

    private boolean signIn(BrokerAuthConfig config, EmbeddedChannel channel) {
        return new BrokerSaslAuthenticator(config, codec, HOST).authenticate(channel);
    }

    private static BrokerAuthConfig iam(String region, String profile) {
        return new BrokerAuthConfig("AWS_MSK_IAM", null, null, region, profile);
    }

    private void useProfileFile() throws IOException {
        useCredentialsFile("""
                [dev]
                aws_access_key_id = %s
                aws_secret_access_key = %s
                aws_session_token = %s
                """.formatted(PROFILE_KEY, PROFILE_SECRET, PROFILE_SESSION));
    }

    private void useCredentialsFile(String content) throws IOException {
        Path file = Files.writeString(dir.resolve("credentials-" + System.nanoTime()), content);
        System.setProperty("aws.sharedCredentialsFile", file.toString());
    }

    private String logText() {
        return logs.list.stream()
                .map(event -> event.getFormattedMessage() + " "
                        + (event.getThrowableProxy() == null ? "" : event.getThrowableProxy().getMessage()))
                .collect(Collectors.joining("\n"));
    }

    private static String readFrame(EmbeddedChannel channel) {
        ByteBuf frame = channel.readOutbound();
        if (frame == null) {
            return "";
        }
        try {
            return frame.toString(StandardCharsets.ISO_8859_1);
        } finally {
            frame.release();
        }
    }

    private static String readAuthenticateFrame(EmbeddedChannel channel) {
        readFrame(channel);
        return readFrame(channel);
    }

    private static String signedCredential(String authenticateFrame) {
        Matcher matcher = CREDENTIAL.matcher(authenticateFrame);
        return matcher.find() ? matcher.group(1) : "";
    }

    private static SaslHandshakeResponseData handshakeOffering(String... mechanisms) {
        var response = new SaslHandshakeResponseData().setErrorCode(Errors.NONE.code());
        for (String mechanism : mechanisms) {
            response.mechanisms().add(mechanism);
        }
        return response;
    }

    private static SaslAuthenticateResponseData accepted() {
        return new SaslAuthenticateResponseData()
                .setErrorCode(Errors.NONE.code())
                .setAuthBytes(ACCEPTED.getBytes(StandardCharsets.UTF_8));
    }

    private static SaslAuthenticateResponseData denied() {
        return new SaslAuthenticateResponseData()
                .setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code())
                .setErrorMessage("Invalid AWS signature");
    }

    /// Creates an [EmbeddedChannel] that answers the SASL handshake and authenticate requests
    /// like a broker with AWS_MSK_IAM enabled, with the given responses.
    private EmbeddedChannel brokerChannel(
            SaslHandshakeResponseData handshakeResponse,
            SaslAuthenticateResponseData authenticateResponse
    ) {
        return new EmbeddedChannel(
                new KafkaFrameDecoder(),
                new KafkaFrameEncoder(),
                new ChannelDuplexHandler() {
                    @Override
                    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                        ByteBuf buf = (ByteBuf) msg;
                        short apiKey = buf.readShort();
                        short apiVersion = buf.readShort();
                        buf.resetReaderIndex();

                        ByteBuf responsePayload = ctx.alloc().buffer();
                        if (apiKey == ApiKeys.SASL_HANDSHAKE.id) {
                            KafkaHeader header = KafkaHeader.of(KafkaApiRegistry.SASL_HANDSHAKE, apiVersion, 1, "test-broker");
                            responseHeaderCodec.encode(responsePayload, header.responseHeaderVersion(), 1);
                            codec.encodeRequest(apiVersion, handshakeResponse, responsePayload);
                        } else if (apiKey == ApiKeys.SASL_AUTHENTICATE.id) {
                            KafkaHeader header = KafkaHeader.of(KafkaApiRegistry.SASL_AUTHENTICATE, apiVersion, 2, "test-broker");
                            responseHeaderCodec.encode(responsePayload, header.responseHeaderVersion(), 2);
                            codec.encodeRequest(apiVersion, authenticateResponse, responsePayload);
                        }

                        ctx.write(msg, promise);
                        promise.addListener((ChannelFutureListener) future -> {
                            ByteBuf framed = ctx.alloc().buffer(Integer.BYTES + responsePayload.readableBytes());
                            framed.writeInt(responsePayload.readableBytes());
                            framed.writeBytes(responsePayload);
                            responsePayload.release();
                            ctx.channel().pipeline().fireChannelRead(framed);
                        });
                    }
                });
    }
}
