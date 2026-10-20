<?php

declare(strict_types=1);

function salebot_amocrm_bridge_env(string $name, string $default = ''): string
{
    $value = getenv($name);
    return $value === false ? $default : trim((string)$value);
}

function salebot_amocrm_bridge_portal_config(string $portalKey = 'directorchinatutorru'): array
{
    $prefix = $portalKey === 'secondary' ? 'AMO_CHAT_SECONDARY_' : 'AMO_CHAT_DIRECTOR_';
    $legacyPrefix = $portalKey === 'secondary' ? $prefix : 'AMO_CHAT_';
    $portals = function_exists('get_config') ? get_config('amocrm_portals') : [];
    $portal = is_array($portals) ? ($portals[$portalKey] ?? []) : [];

    return [
        'portal_key' => $portalKey,
        'enabled' => filter_var(salebot_amocrm_bridge_env($prefix . 'ENABLED', '0'), FILTER_VALIDATE_BOOLEAN),
        'subdomain' => (string)($portal['subdomain'] ?? ''),
        'channel_id' => salebot_amocrm_bridge_env($prefix . 'CHANNEL_ID', salebot_amocrm_bridge_env($legacyPrefix . 'CHANNEL_ID')),
        'channel_secret' => salebot_amocrm_bridge_env($prefix . 'CHANNEL_SECRET', salebot_amocrm_bridge_env($legacyPrefix . 'CHANNEL_SECRET')),
        'scope_id' => salebot_amocrm_bridge_env($prefix . 'SCOPE_ID'),
        'account_id' => salebot_amocrm_bridge_env($prefix . 'ACCOUNT_ID', salebot_amocrm_bridge_env($legacyPrefix . 'ACCOUNT_ID')),
        'bot_ref_id' => salebot_amocrm_bridge_env($prefix . 'BOT_REF_ID', salebot_amocrm_bridge_env($legacyPrefix . 'BOT_REF_ID')),
        'state_dir' => salebot_amocrm_bridge_env('AMO_CHAT_STATE_DIR', sys_get_temp_dir() . '/salebot_chat_bridge'),
    ];
}

function salebot_amocrm_bridge_is_configured(array $config): bool
{
    foreach (['enabled', 'subdomain', 'channel_id', 'channel_secret', 'scope_id', 'account_id', 'bot_ref_id'] as $key) {
        if (empty($config[$key])) {
            return false;
        }
    }
    return true;
}

function salebot_amocrm_bridge_signature(string $body, string $secret): string
{
    return hash_hmac('sha1', $body, $secret);
}

function salebot_amocrm_bridge_encode_client_id(string $clientId): string
{
    return rtrim(strtr(base64_encode($clientId), '+/', '-_'), '=');
}

function salebot_amocrm_bridge_decode_client_id(string $encoded): ?string
{
    $padding = (4 - strlen($encoded) % 4) % 4;
    $decoded = base64_decode(strtr($encoded, '-_', '+/') . str_repeat('=', $padding), true);
    return is_string($decoded) && $decoded !== '' ? $decoded : null;
}

function salebot_amocrm_bridge_conversation_id(string $clientId): string
{
    return 'sb-conv-' . salebot_amocrm_bridge_encode_client_id($clientId);
}

function salebot_amocrm_bridge_user_id(string $clientId): string
{
    return 'sb-user-' . salebot_amocrm_bridge_encode_client_id($clientId);
}

function salebot_amocrm_bridge_extract_client_id(array $message): ?string
{
    foreach ([$message['receiver']['client_id'] ?? null, $message['conversation']['client_id'] ?? null] as $candidate) {
        if ((!is_string($candidate) && !is_numeric($candidate)) || !preg_match('/^sb-(?:user|conv)-([A-Za-z0-9_-]+)$/', (string)$candidate, $matches)) {
            continue;
        }
        return salebot_amocrm_bridge_decode_client_id($matches[1]);
    }
    return null;
}

function salebot_amocrm_bridge_message_id(array $event): string
{
    $messageId = $event['id'] ?? null;
    if ((is_string($messageId) && trim($messageId) !== '') || is_numeric($messageId)) {
        return 'sb-' . hash('sha256', (string)($event['project_id'] ?? '') . ':' . (string)$messageId);
    }
    return 'sb-' . bin2hex(random_bytes(20));
}

function salebot_amocrm_bridge_build_message_payload(
    array $event,
    string $clientId,
    string $conversationId,
    string $chatId,
    string $clientName,
    string $botRefId,
    string $userRefId = '',
    string $phone = '',
    string $email = ''
): array {
    $text = trim((string)($event['message'] ?? ''));
    $isIncoming = (string)($event['is_input'] ?? '1') === '1';
    $timestampValue = $event['timestamp'] ?? null;
    $timestamp = is_numeric($timestampValue)
        ? (int)$timestampValue
        : (is_string($timestampValue) ? (strtotime($timestampValue) ?: 0) : 0);
    if ($timestamp > 1000000000000) {
        $timestamp = (int)floor($timestamp / 1000);
    }
    if ($timestamp <= 0) {
        $timestamp = time();
    }

    $client = [
        'id' => salebot_amocrm_bridge_user_id($clientId),
        'name' => $clientName !== '' ? $clientName : 'Клиент Salebot',
    ];
    $profile = [];
    if ($phone !== '' && $phone !== '.') {
        $profile['phone'] = $phone;
    }
    if ($email !== '' && $email !== '.') {
        $profile['email'] = $email;
    }
    if ($profile) {
        $client['profile'] = $profile;
    }

    $payload = [
        'timestamp' => $timestamp,
        'msec_timestamp' => $timestamp * 1000,
        'msgid' => salebot_amocrm_bridge_message_id($event),
        'conversation_id' => $conversationId,
        'conversation_ref_id' => $chatId,
        'message' => ['type' => 'text', 'text' => $text],
        'silent' => false,
    ];
    if ($isIncoming) {
        $client['ref_id'] = $userRefId;
        $payload['sender'] = $client;
    } else {
        $payload['sender'] = ['id' => 'salebot-integration-bot', 'ref_id' => $botRefId, 'name' => 'Salebot'];
        $client['ref_id'] = $userRefId;
        $payload['receiver'] = $client;
    }

    return ['event_type' => 'new_message', 'payload' => $payload];
}

function salebot_amocrm_bridge_outgoing_hash(string $text): string
{
    return hash('sha256', trim($text));
}

function salebot_amocrm_bridge_state_path(array $config, string $clientId): string
{
    $namespace = (string)($config['portal_key'] ?? 'default');
    return rtrim((string)$config['state_dir'], '/') . '/' . hash('sha256', $namespace . ':' . $clientId) . '.json';
}

function salebot_amocrm_bridge_update_state(array $config, string $clientId, callable $update): array
{
    $directory = (string)$config['state_dir'];
    if (!is_dir($directory) && !mkdir($directory, 0700, true) && !is_dir($directory)) {
        throw new RuntimeException('Could not create chat bridge state directory.');
    }
    $path = salebot_amocrm_bridge_state_path($config, $clientId);
    $handle = fopen($path, 'c+');
    if ($handle === false) {
        throw new RuntimeException('Could not open chat bridge state.');
    }
    try {
        if (!flock($handle, LOCK_EX)) {
            throw new RuntimeException('Could not lock chat bridge state.');
        }
        $contents = stream_get_contents($handle);
        $state = is_string($contents) ? json_decode($contents, true) : null;
        $state = is_array($state) ? $state : [];
        $state = $update($state);
        ftruncate($handle, 0);
        rewind($handle);
        fwrite($handle, json_encode($state, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
        fflush($handle);
        @chmod($path, 0600);
        return $state;
    } finally {
        flock($handle, LOCK_UN);
        fclose($handle);
    }
}

function salebot_amocrm_bridge_read_state(array $config, string $clientId): array
{
    $path = salebot_amocrm_bridge_state_path($config, $clientId);
    if (!is_file($path)) {
        return [];
    }
    $decoded = json_decode((string)file_get_contents($path), true);
    return is_array($decoded) ? $decoded : [];
}

function salebot_amocrm_bridge_has_seen(array $config, string $clientId, string $messageId): bool
{
    $state = salebot_amocrm_bridge_read_state($config, $clientId);
    $ids = str_starts_with($messageId, 'amo:') ? ($state['amo_message_ids'] ?? []) : ($state['salebot_message_ids'] ?? []);
    return in_array($messageId, $ids, true);
}

function salebot_amocrm_bridge_mark_salebot_seen(array $config, string $clientId, string $messageId): void
{
    salebot_amocrm_bridge_update_state($config, $clientId, static function (array $state) use ($messageId): array {
        $ids = array_values(array_filter($state['salebot_message_ids'] ?? [], 'is_string'));
        $ids[] = $messageId;
        $state['salebot_message_ids'] = array_slice(array_values(array_unique($ids)), -200);
        return $state;
    });
}

function salebot_amocrm_bridge_mark_seen(array $config, string $clientId, string $messageId): void
{
    salebot_amocrm_bridge_update_state($config, $clientId, static function (array $state) use ($messageId): array {
        $ids = array_values(array_filter($state['amo_message_ids'] ?? [], 'is_string'));
        $ids[] = $messageId;
        $state['amo_message_ids'] = array_slice(array_values(array_unique($ids)), -200);
        return $state;
    });
}

function salebot_amocrm_bridge_queue_echo(array $config, string $clientId, string $text): void
{
    salebot_amocrm_bridge_update_state($config, $clientId, static function (array $state) use ($text): array {
        $queue = array_values(array_filter($state['outgoing_echoes'] ?? [], 'is_array'));
        $queue[] = ['hash' => salebot_amocrm_bridge_outgoing_hash($text), 'at' => time()];
        $state['outgoing_echoes'] = array_slice($queue, -20);
        return $state;
    });
}

function salebot_amocrm_bridge_consume_echo(array $config, string $clientId, string $text): bool
{
    $matched = false;
    salebot_amocrm_bridge_update_state($config, $clientId, static function (array $state) use ($text, &$matched): array {
        $hash = salebot_amocrm_bridge_outgoing_hash($text);
        $queue = [];
        foreach (($state['outgoing_echoes'] ?? []) as $echo) {
            if (!is_array($echo) || (int)($echo['at'] ?? 0) < time() - 300) {
                continue;
            }
            if (!$matched && hash_equals((string)($echo['hash'] ?? ''), $hash)) {
                $matched = true;
                continue;
            }
            $queue[] = $echo;
        }
        $state['outgoing_echoes'] = $queue;
        return $state;
    });
    return $matched;
}

function salebot_amocrm_bridge_http_request(string $method, string $url, array $headers = [], ?string $body = null, int $timeout = 12): array
{
    $curl = curl_init($url);
    $options = [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_CUSTOMREQUEST => strtoupper($method),
        CURLOPT_HTTPHEADER => $headers,
        CURLOPT_TIMEOUT => $timeout,
        CURLOPT_CONNECTTIMEOUT => 5,
        CURLOPT_SSL_VERIFYPEER => true,
        CURLOPT_SSL_VERIFYHOST => 2,
    ];
    if ($body !== null) {
        $options[CURLOPT_POSTFIELDS] = $body;
    }
    curl_setopt_array($curl, $options);
    $responseBody = curl_exec($curl);
    $curlError = curl_error($curl);
    $status = (int)curl_getinfo($curl, CURLINFO_HTTP_CODE);
    curl_close($curl);
    $decoded = is_string($responseBody) ? json_decode($responseBody, true) : null;
    return [
        'ok' => $curlError === '' && $status >= 200 && $status < 300,
        'status' => $status,
        'error' => $curlError,
        'data' => is_array($decoded) ? $decoded : null,
        'body' => (string)$responseBody,
    ];
}

function salebot_amocrm_bridge_amo_api(array $portalConfig, array $accessData, string $method, string $path, ?array $payload = null): array
{
    $token = trim((string)($accessData['access_token'] ?? ''));
    $subdomain = trim((string)($portalConfig['subdomain'] ?? ''));
    if ($token === '' || $subdomain === '') {
        return ['ok' => false, 'status' => 0, 'error' => 'amoCRM access token or subdomain missing', 'data' => null, 'body' => ''];
    }
    $body = $payload === null ? null : json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    $headers = ['Authorization: Bearer ' . $token, 'Accept: application/json'];
    if ($body !== null) {
        $headers[] = 'Content-Type: application/json';
    }
    return salebot_amocrm_bridge_http_request($method, 'https://' . $subdomain . '.amocrm.ru' . $path, $headers, $body);
}

function salebot_amocrm_bridge_amojo_request(array $config, string $path, array $payload): array
{
    $body = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    if (!is_string($body)) {
        return ['ok' => false, 'status' => 0, 'error' => 'Could not encode amojo payload', 'data' => null, 'body' => ''];
    }
    $date = gmdate('D, d M Y H:i:s') . ' +0000';
    $checksum = md5($body);
    $signatureBase = implode("\n", ['POST', $checksum, 'application/json', $date, $path]);
    $signature = salebot_amocrm_bridge_signature($signatureBase, (string)$config['channel_secret']);
    return salebot_amocrm_bridge_http_request('POST', 'https://amojo.amocrm.ru' . $path, [
        'Date: ' . $date,
        'Content-Type: application/json',
        'Content-MD5: ' . strtolower($checksum),
        'X-Signature: ' . strtolower($signature),
        'User-Agent: ChinaTutor-Salebot-Chat-Bridge/1.0',
    ], $body);
}

function salebot_amocrm_bridge_connect_channel(array $config, string $hookApiVersion = 'v2'): array
{
    if (empty($config['channel_id']) || empty($config['channel_secret']) || empty($config['account_id'])) {
        return ['ok' => false, 'error' => 'Channel id, secret, and amojo account id are required'];
    }
    if (!in_array($hookApiVersion, ['v1', 'v2'], true)) {
        return ['ok' => false, 'error' => 'Unsupported webhook version'];
    }
    $path = '/v2/origin/custom/' . rawurlencode((string)$config['channel_id']) . '/connect';
    $response = salebot_amocrm_bridge_amojo_request($config, $path, [
        'account_id' => (string)$config['account_id'],
        'hook_api_version' => $hookApiVersion,
        'title' => 'ChinaTutor Salebot',
    ]);
    if (!$response['ok']) {
        return ['ok' => false, 'status' => $response['status'], 'error' => substr($response['body'] ?: $response['error'], 0, 1000)];
    }
    $scopeId = trim((string)($response['data']['scope_id'] ?? ''));
    return ['ok' => $scopeId !== '', 'scope_id' => $scopeId, 'response' => $response['data']];
}

function salebot_amocrm_bridge_access_data(string $portalKey): array
{
    // Chat contact-link requests may use a separate OAuth application/token
    // from the legacy lead synchronization integration.
    $chatAccessToken = trim((string)(getenv('AMO_CHAT_' . strtoupper($portalKey === 'secondary' ? 'SECONDARY' : 'DIRECTOR') . '_ACCESS_TOKEN') ?: ''));
    if ($chatAccessToken !== '') {
        return ['access_token' => $chatAccessToken, 'time' => time()];
    }
    if ($portalKey === 'directorchinatutorru') {
        return is_array($GLOBALS['data'] ?? null) ? $GLOBALS['data'] : [];
    }
    require_once __DIR__ . '/yandex_forms_common.php';
    return yandex_forms_portal_access_data(yandex_forms_portal_config($portalKey));
}

function salebot_amocrm_bridge_resolve_contact_id(array $portalConfig, array $accessData, $leadId, $contactId): ?int
{
    if ((int)$contactId > 0) {
        return (int)$contactId;
    }
    if ((int)$leadId <= 0) {
        return null;
    }
    $lead = salebot_amocrm_bridge_amo_api($portalConfig, $accessData, 'GET', '/api/v4/leads/' . (int)$leadId . '?with=contacts');
    foreach (($lead['data']['_embedded']['contacts'] ?? []) as $contact) {
        if ((int)($contact['id'] ?? 0) > 0) {
            return (int)$contact['id'];
        }
    }
    return null;
}

function salebot_amocrm_bridge_ensure_chat(array $config, array $portalConfig, array $accessData, string $clientId, string $clientName, string $phone, string $email, $leadId, $contactId): array
{
    $conversationId = salebot_amocrm_bridge_conversation_id($clientId);
    $resolvedContactId = salebot_amocrm_bridge_resolve_contact_id($portalConfig, $accessData, $leadId, $contactId);
    if (!$resolvedContactId) {
        return ['ok' => false, 'step' => 'resolve_contact', 'error' => 'No linked amoCRM contact ID'];
    }

    $user = [
        'id' => salebot_amocrm_bridge_user_id($clientId),
        'name' => $clientName !== '' ? $clientName : 'Клиент Salebot',
    ];
    if ($phone !== '' && $phone !== '.') {
        $user['profile']['phone'] = $phone;
    }
    if ($email !== '' && $email !== '.') {
        $user['profile']['email'] = $email;
    }
    $path = '/v2/origin/custom/' . (string)$config['scope_id'] . '/chats';
    $chat = salebot_amocrm_bridge_amojo_request($config, $path, [
        'account_id' => (string)$config['account_id'],
        'conversation_id' => $conversationId,
        'user' => $user,
    ]);
    $chatId = (string)($chat['data']['id'] ?? '');
    if (!$chat['ok'] || $chatId === '') {
        return ['ok' => false, 'step' => 'create_chat', 'status' => $chat['status'], 'error' => $chat['body'] ?: $chat['error']];
    }

    $query = 'chat_id%5B%5D=' . rawurlencode($chatId);
    $links = salebot_amocrm_bridge_amo_api($portalConfig, $accessData, 'GET', '/api/v4/contacts/chats?' . $query);
    if (!$links['ok']) {
        return ['ok' => false, 'step' => 'check_contact_link', 'status' => $links['status'], 'error' => $links['body'] ?: $links['error'], 'chat_id' => $chatId];
    }
    $alreadyLinked = false;
    foreach (($links['data']['_embedded']['chats'] ?? []) as $link) {
        if ((string)($link['chat_id'] ?? '') !== $chatId) {
            continue;
        }
        if ((int)($link['contact_id'] ?? 0) === $resolvedContactId) {
            $alreadyLinked = true;
        } else {
            return ['ok' => false, 'step' => 'link_conflict', 'chat_id' => $chatId, 'error' => 'Chat is already linked to a different amoCRM contact'];
        }
    }
    if (!$alreadyLinked) {
        $linked = salebot_amocrm_bridge_amo_api($portalConfig, $accessData, 'POST', '/api/v4/contacts/chats', [[
            'contact_id' => $resolvedContactId,
            'chat_id' => $chatId,
        ]]);
        if (!$linked['ok']) {
            return ['ok' => false, 'step' => 'link_contact', 'status' => $linked['status'], 'error' => $linked['body'] ?: $linked['error'], 'chat_id' => $chatId];
        }
    }
    return [
        'ok' => true,
        'chat_id' => $chatId,
        'user_ref_id' => (string)($chat['data']['user']['id'] ?? ''),
        'conversation_id' => $conversationId,
        'contact_id' => $resolvedContactId,
        'already_linked' => $alreadyLinked,
    ];
}

function salebot_amocrm_bridge_save_salebot_variables(string $apiKey, string $clientId, array $variables): array
{
    if ($apiKey === '' || $clientId === '' || !$variables) {
        return ['ok' => false, 'reason' => 'missing_api_key_client_or_variables'];
    }
    $response = salebot_amocrm_bridge_http_request('POST', 'https://chatter.salebot.pro/api/' . rawurlencode($apiKey) . '/save_variables', [
        'Content-Type: application/json',
        'Accept: application/json',
    ], json_encode(['client_id' => $clientId, 'variables' => $variables], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
    return ['ok' => salebot_amocrm_bridge_salebot_response_succeeded($response), 'status' => $response['status']];
}

function salebot_amocrm_bridge_salebot_response_succeeded(array $response): bool
{
    if (empty($response['ok'])) {
        return false;
    }
    $data = $response['data'] ?? null;
    if (!is_array($data)) {
        return true;
    }
    if (($data['status'] ?? null) !== null && $data['status'] !== 'success') {
        return false;
    }
    if (($data['success'] ?? null) === false || !empty($data['error'])) {
        return false;
    }
    return true;
}

function salebot_amocrm_bridge_log(string $message, array $context = []): void
{
    if (function_exists('log_info')) {
        log_info($message, $context, 'salebot_amocrm_chat_bridge.php');
        return;
    }
    error_log($message . ' ' . json_encode($context, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
}

function salebot_amocrm_bridge_process_salebot_event(
    array $event,
    string $apiKey,
    string $portalKey = 'directorchinatutorru',
    $resolvedLeadId = null,
    $resolvedContactId = null
): array
{
    $clientId = (string)($event['client']['id'] ?? $event['client']['client_id'] ?? '');
    $text = trim((string)($event['message'] ?? ''));
    if ($clientId === '' || $text === '') {
        return ['skipped' => true, 'reason' => 'not_a_client_text_message'];
    }
    if ((string)($event['is_input'] ?? '1') !== '1'
        && array_key_exists('delivered', $event)
        && !in_array(strtolower((string)$event['delivered']), ['1', 'true'], true)) {
        return ['skipped' => true, 'reason' => 'salebot_message_not_delivered'];
    }
    if (!in_array($portalKey, ['directorchinatutorru', 'secondary'], true)) {
        return ['skipped' => true, 'reason' => 'unknown_amo_portal'];
    }
    if ($apiKey === '') {
        return ['skipped' => true, 'reason' => 'chat_bridge_salebot_api_key_missing'];
    }
    try {
        $eventVariables = $event['client']['variables'] ?? [];
        $eventOrderVariables = $event['client']['order_variables'] ?? [];
        $portalVariable = (string)(
            $eventVariables['amo_portal']
            ?? $eventOrderVariables['amo_portal']
            ?? ''
        );
        if ($portalVariable !== '') {
            $portalKey = in_array($portalVariable, ['supportchinatutorru', 'secondary'], true)
                ? 'secondary'
                : $portalVariable;
        }
        if (!in_array($portalKey, ['directorchinatutorru', 'secondary'], true)) {
            return ['skipped' => true, 'reason' => 'unknown_amo_portal'];
        }
        $config = salebot_amocrm_bridge_portal_config($portalKey);
        if (!salebot_amocrm_bridge_is_configured($config)) {
            return ['skipped' => true, 'reason' => 'chat_channel_not_configured'];
        }
        $variables = array_replace($eventOrderVariables, $eventVariables);
        $hasLinkedAmoRecord = $resolvedLeadId
            || $resolvedContactId
            || !empty($variables['amo_lead_id'])
            || !empty($variables['amo_client_id'])
            || !empty($variables['amo_lead_id_secondary'])
            || !empty($variables['amo_client_id_secondary']);
        if (!$hasLinkedAmoRecord) {
            $variablesResponse = salebot_amocrm_bridge_http_request('GET', 'https://chatter.salebot.pro/api/' . rawurlencode($apiKey) . '/get_variables?' . http_build_query(['client_id' => $clientId]), ['Accept: application/json']);
            $storedVariables = is_array($variablesResponse['data']['variables'] ?? null) ? $variablesResponse['data']['variables'] : ($variablesResponse['data'] ?? []);
            if (is_array($storedVariables)) {
                $variables = array_replace($storedVariables, $variables);
            }
        }
        if ($portalVariable === '') {
            $portalVariable = (string)($variables['amo_portal'] ?? $variables['client.amo_portal'] ?? '');
            if ($portalVariable !== '') {
                $portalKey = in_array($portalVariable, ['supportchinatutorru', 'secondary'], true)
                    ? 'secondary'
                    : $portalVariable;
                if (!in_array($portalKey, ['directorchinatutorru', 'secondary'], true)) {
                    return ['skipped' => true, 'reason' => 'unknown_amo_portal'];
                }
                $config = salebot_amocrm_bridge_portal_config($portalKey);
                if (!salebot_amocrm_bridge_is_configured($config)) {
                    return ['skipped' => true, 'reason' => 'chat_channel_not_configured'];
                }
            }
        }
        $messageId = salebot_amocrm_bridge_message_id($event);
        if (salebot_amocrm_bridge_has_seen($config, $clientId, $messageId)) {
            return ['skipped' => true, 'reason' => 'duplicate_salebot_message'];
        }
        if ((string)($event['is_input'] ?? '1') !== '1' && salebot_amocrm_bridge_consume_echo($config, $clientId, $text)) {
            salebot_amocrm_bridge_mark_salebot_seen($config, $clientId, $messageId);
            return ['skipped' => true, 'reason' => 'amo_outgoing_echo'];
        }
        if ($portalKey === 'secondary') {
            $leadId = $variables['amo_lead_id_secondary'] ?? $variables['client.amo_lead_id_secondary'] ?? null;
            $contactId = $variables['amo_client_id_secondary'] ?? $variables['client.amo_client_id_secondary'] ?? null;
        } else {
            $leadId = $resolvedLeadId ?: ($variables['amo_lead_id'] ?? $variables['client.amo_lead_id'] ?? $eventOrderVariables['amo_lead_id'] ?? $eventVariables['amo_lead_id'] ?? null);
            $contactId = $resolvedContactId ?: ($variables['amo_client_id'] ?? $variables['client.amo_client_id'] ?? $eventVariables['amo_client_id'] ?? null);
        }
        if (!$leadId && !$contactId) {
            return ['skipped' => true, 'reason' => 'amo_contact_not_linked_yet'];
        }

        if (!function_exists('yandex_forms_portal_config')) {
            require_once __DIR__ . '/yandex_forms_common.php';
        }
        $portalConfig = yandex_forms_portal_config($portalKey);
        $accessData = salebot_amocrm_bridge_access_data($portalKey);
        $name = trim((string)($variables['name'] ?? $event['client']['name'] ?? 'Клиент Salebot'));
        $phone = trim((string)($variables['phone'] ?? $variables['tel'] ?? ''));
        $email = trim((string)($variables['email'] ?? ''));
        $chat = salebot_amocrm_bridge_ensure_chat($config, $portalConfig, $accessData, $clientId, $name, $phone, $email, $leadId, $contactId);
        if (!$chat['ok']) {
            salebot_amocrm_bridge_log('Failed to prepare amoCRM chat', $chat + ['portal' => $portalKey]);
            return $chat;
        }

        $suffix = $portalKey === 'directorchinatutorru' ? 'director' : $portalKey;
        $chatVariables = [
            'amo_chat_id_' . $suffix => $chat['chat_id'],
            'amo_conversation_ref_id_' . $suffix => $chat['chat_id'],
            'amo_chat_conversation_id_' . $suffix => $chat['conversation_id'],
            'client.amo_chat_id' => $chat['chat_id'],
            'client.amo_conversation_ref_id' => $chat['chat_id'],
        ];
        $alreadySaved = true;
        foreach ($chatVariables as $key => $value) {
            if ((string)($variables[$key] ?? '') !== (string)$value) {
                $alreadySaved = false;
                break;
            }
        }
        $saved = $alreadySaved
            ? ['ok' => true, 'skipped' => true]
            : salebot_amocrm_bridge_save_salebot_variables($apiKey, $clientId, $chatVariables);

        $messagePayload = salebot_amocrm_bridge_build_message_payload(
            $event,
            $clientId,
            $chat['conversation_id'],
            $chat['chat_id'],
            $name,
            (string)$config['bot_ref_id'],
            $chat['user_ref_id'],
            $phone,
            $email
        );
        $sent = salebot_amocrm_bridge_amojo_request($config, '/v2/origin/custom/' . (string)$config['scope_id'], $messagePayload);
        if (!$sent['ok']) {
            salebot_amocrm_bridge_log('Failed to send Salebot message to amoCRM', [
                'status' => $sent['status'],
                'response' => substr($sent['body'], 0, 800),
                'salebot_message_id' => $messageId,
            ]);
            return ['ok' => false, 'step' => 'send_to_amocrm', 'status' => $sent['status']];
        }
        salebot_amocrm_bridge_mark_salebot_seen($config, $clientId, $messageId);
        return ['ok' => true, 'chat_id' => $chat['chat_id'], 'contact_id' => $chat['contact_id'], 'salebot_ids_saved' => $saved['ok']];
    } catch (Throwable $exception) {
        salebot_amocrm_bridge_log('Error processing Salebot message', [
            'error' => $exception->getMessage(),
            'client_id_hash' => hash('sha256', $clientId),
            'portal' => $portalKey,
        ]);
        return ['ok' => false, 'error' => $exception->getMessage()];
    }
}

function salebot_amocrm_bridge_update_delivery_status(array $config, string $scopeId, string $messageId, int $statusCode, int $errorCode = 0, string $error = ''): array
{
    return salebot_amocrm_bridge_amojo_request($config, '/v2/origin/custom/' . $scopeId . '/' . rawurlencode($messageId) . '/delivery_status', [
        'status_code' => $statusCode,
        'error_code' => $errorCode,
        'error' => $error,
    ]);
}

function salebot_amocrm_bridge_handle_amo_webhook(string $rawBody, string $signature, string $scopeId, string $apiKey): array
{
    $hook = json_decode($rawBody, true);
    if (!is_array($hook)) {
        return ['http_status' => 400, 'error' => 'Invalid JSON'];
    }

    $config = null;
    $portalKey = '';
    $accountId = (string)($hook['account_id'] ?? '');
    foreach (['directorchinatutorru', 'secondary'] as $candidateKey) {
        $candidate = salebot_amocrm_bridge_portal_config($candidateKey);
        if ($accountId !== ''
            && $scopeId !== ''
            && hash_equals((string)$candidate['account_id'], $accountId)
            && hash_equals((string)$candidate['scope_id'], $scopeId)) {
            $config = $candidate;
            $portalKey = $candidateKey;
            break;
        }
    }
    if (!is_array($config) || !salebot_amocrm_bridge_is_configured($config)) {
        salebot_amocrm_bridge_log('Rejected amoCRM chat webhook: unknown account or scope', [
            'account_id' => $accountId,
            'scope_id' => $scopeId,
        ]);
        return ['http_status' => 404, 'error' => 'Unknown or disabled amoCRM chat account'];
    }
    if (!hash_equals(salebot_amocrm_bridge_signature(trim($rawBody), (string)$config['channel_secret']), strtolower(trim($signature)))) {
        salebot_amocrm_bridge_log('Rejected amoCRM chat webhook: invalid signature', ['portal' => $portalKey]);
        return ['http_status' => 401, 'error' => 'Invalid X-Signature'];
    }
    $scopeId = (string)$config['scope_id'];
    $message = is_array($hook['message'] ?? null) ? $hook['message'] : [];
    $text = trim((string)($message['message']['text'] ?? ''));
    $messageId = trim((string)($message['message']['id'] ?? ''));
    $clientId = salebot_amocrm_bridge_extract_client_id($message);
    if ($clientId === null || $text === '' || $messageId === '') {
        salebot_amocrm_bridge_log('Rejected amoCRM chat webhook: unsupported message', [
            'portal' => $portalKey,
            'has_client_id' => $clientId !== null,
            'has_text' => $text !== '',
            'has_message_id' => $messageId !== '',
        ]);
        return ['http_status' => 400, 'error' => 'Unsupported or incomplete chat webhook'];
    }
    $stateConfig = $config;
    if (salebot_amocrm_bridge_has_seen($stateConfig, $clientId, 'amo:' . $messageId)) {
        return ['http_status' => 200, 'sent' => true, 'duplicate' => true];
    }

    salebot_amocrm_bridge_queue_echo($stateConfig, $clientId, $text);
    $salebot = salebot_amocrm_bridge_http_request('POST', 'https://chatter.salebot.pro/api/' . rawurlencode($apiKey) . '/message', [
        'Content-Type: application/json',
        'Accept: application/json',
    ], json_encode(['client_id' => $clientId, 'message' => $text], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
    $sent = salebot_amocrm_bridge_salebot_response_succeeded($salebot);
    $delivery = null;
    if (!$sent) {
        $delivery = salebot_amocrm_bridge_update_delivery_status(
            $config,
            $scopeId,
            $messageId,
            -1,
            $salebot['status'] === 0 || $salebot['status'] >= 500 ? 903 : 905,
            'Salebot did not accept the message (HTTP ' . $salebot['status'] . ')'
        );
    }
    if ($sent) {
        salebot_amocrm_bridge_mark_seen($stateConfig, $clientId, 'amo:' . $messageId);
        salebot_amocrm_bridge_log('Delivered amoCRM chat message to Salebot', [
            'portal' => $portalKey,
            'message_id' => $messageId,
            'chat_id' => (string)($message['conversation']['id'] ?? ''),
            'salebot_status' => $salebot['status'],
        ]);
    } else {
        salebot_amocrm_bridge_consume_echo($stateConfig, $clientId, $text);
        salebot_amocrm_bridge_log('Failed to deliver amoCRM message to Salebot', [
            'portal' => $portalKey,
            'status' => $salebot['status'],
            'response' => substr($salebot['body'], 0, 800),
            'delivery_status_updated' => $delivery['ok'] ?? false,
        ]);
    }
    return ['http_status' => 200, 'sent' => $sent];
}
