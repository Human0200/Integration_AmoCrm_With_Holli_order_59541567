<?php

declare(strict_types=1);

require_once __DIR__ . '/bootstrap.php';
$bridgeSource = file_get_contents(tests_project_root() . '/salebot_amocrm_chat_bridge.php');
if ($bridgeSource === false) {
    throw new RuntimeException('Could not read chat bridge source.');
}
eval(substr($bridgeSource, strpos($bridgeSource, 'function salebot_amocrm_bridge_env')));

$clientId = 'client-with:unsafe/id';
$conversationId = salebot_amocrm_bridge_conversation_id($clientId);
$userId = salebot_amocrm_bridge_user_id($clientId);
tests_assert_true(str_starts_with($conversationId, 'sb-conv-'), 'Conversation id should have the integration prefix.');
tests_assert_true(str_starts_with($userId, 'sb-user-'), 'Participant id should have the integration prefix.');
tests_assert_same($clientId, salebot_amocrm_bridge_extract_client_id([
    'conversation' => ['client_id' => $conversationId],
]), 'Salebot id should round-trip through the chat conversation id.');
tests_assert_same('a18991ff7e4513a1c2d2ee51e3a8e99ca891d9cd', salebot_amocrm_bridge_signature('body', 'secret'), 'Channel signatures should use HMAC-SHA1.');
tests_assert_true(salebot_amocrm_bridge_is_configured([
    'enabled' => true,
    'subdomain' => 'example',
    'channel_id' => 'channel',
    'channel_secret' => 'secret',
    'scope_id' => 'scope',
    'account_id' => 'account',
    'bot_ref_id' => 'bot',
]), 'Issued Chat API credentials should be sufficient to configure the bridge.');

$incoming = salebot_amocrm_bridge_build_message_payload(
    ['id' => 'm-1', 'message' => 'Hello', 'is_input' => 1, 'timestamp' => 1780000000, 'client' => ['id' => 'sb-client-1']],
    $clientId,
    $conversationId,
    'chat-uuid',
    'Client',
    'bot-ref',
    'amo-user-ref',
    '.',
    '.'
);
tests_assert_same('text', $incoming['payload']['message']['type'], 'Only text messages should be sent in this implementation.');
tests_assert_same('chat-uuid', $incoming['payload']['conversation_ref_id'], 'AmoCRM chat UUID should be preserved.');
tests_assert_same('amo-user-ref', $incoming['payload']['sender']['ref_id'], 'AmoCRM user reference should be assigned as client sender.');
tests_assert_true(!isset($incoming['payload']['sender']['profile']), 'Placeholder dot must not be passed as phone/email.');
tests_assert_same(1780000000000, $incoming['payload']['msec_timestamp'], 'Message timestamp should use the actual event timestamp.');

$outgoing = salebot_amocrm_bridge_build_message_payload(
    ['id' => 'm-2', 'message' => 'Reply', 'is_input' => 0],
    $clientId,
    $conversationId,
    'chat-uuid',
    'Client',
    'bot-ref',
    'amo-user-ref'
);
tests_assert_same('bot-ref', $outgoing['payload']['sender']['ref_id'], 'Amo-originated message should use the registered bot ref id.');
tests_assert_same($userId, $outgoing['payload']['receiver']['id'], 'Amo-originated message should target the Salebot participant id.');
tests_assert_true($outgoing['payload']['timestamp'] >= time() - 2, 'Client creation time must not be mistaken for the message timestamp.');
tests_assert_true(salebot_amocrm_bridge_salebot_response_succeeded(['ok' => true, 'data' => null]), 'A successful HTTP response without a JSON status should be accepted.');
tests_assert_true(!salebot_amocrm_bridge_salebot_response_succeeded(['ok' => true, 'data' => ['status' => 'error']]), 'Explicit Salebot API errors should be rejected.');
tests_assert_true(!salebot_amocrm_bridge_salebot_response_succeeded(['ok' => true, 'data' => ['success' => false]]), 'Explicit false success responses should be rejected.');

$eventId = salebot_amocrm_bridge_message_id(['id' => '123', 'project_id' => 'p1']);
tests_assert_same($eventId, salebot_amocrm_bridge_message_id(['id' => '123', 'project_id' => 'p1']), 'Message id should be stable for webhook retries.');
tests_assert_true(
    salebot_amocrm_bridge_message_id(['message_id' => 'block-1', 'message' => 'first'])
        !== salebot_amocrm_bridge_message_id(['message_id' => 'block-1', 'message' => 'second']),
    'Salebot block ids must not be treated as unique message ids.'
);
tests_assert_true(salebot_amocrm_bridge_message_id(['client' => ['id' => 'c1'], 'message' => 'same']) !== salebot_amocrm_bridge_message_id(['client' => ['id' => 'c1'], 'message' => 'same']), 'Missing source IDs must not collapse distinct identical messages.');

$stateDir = sys_get_temp_dir() . '/codex_chat_bridge_tests_' . getmypid();
$config = ['state_dir' => $stateDir];
salebot_amocrm_bridge_mark_seen($config, $clientId, 'amo:message-1');
tests_assert_true(salebot_amocrm_bridge_has_seen($config, $clientId, 'amo:message-1'), 'Seen amoCRM message ids should persist.');
salebot_amocrm_bridge_queue_echo($config, $clientId, 'sent by manager');
tests_assert_true(salebot_amocrm_bridge_consume_echo($config, $clientId, 'sent by manager'), 'Manager message echo should be consumed once.');
tests_assert_true(!salebot_amocrm_bridge_consume_echo($config, $clientId, 'sent by manager'), 'Echo marker should not be reusable.');

echo "OK\n";
