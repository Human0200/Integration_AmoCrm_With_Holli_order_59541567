<?php

declare(strict_types=1);

require_once __DIR__ . '/config.php';
require_once __DIR__ . '/logger.php';
require_once __DIR__ . '/amo_func.php';

function yandex_forms_config(): array
{
    $config = get_config('yandex_forms');
    return is_array($config) ? $config : [];
}

function yandex_forms_extract_query_params(array $payload): array
{
    $queryParams = $payload['query_params'] ?? [];

    if (is_string($queryParams) && trim($queryParams) !== '') {
        $decoded = json_decode($queryParams, true);
        if (is_array($decoded)) {
            $queryParams = $decoded;
        } else {
            parse_str($queryParams, $parsedQueryParams);
            $queryParams = is_array($parsedQueryParams) ? $parsedQueryParams : [];
        }
    }

    return is_array($queryParams) ? $queryParams : [];
}

function yandex_forms_normalize_portal_key(string $rawPortalKey): string
{
    $rawPortalKey = trim($rawPortalKey);
    if ($rawPortalKey === '') {
        return '';
    }

    $portals = get_config('amocrm_portals');
    if (!is_array($portals)) {
        return '';
    }

    if (isset($portals[$rawPortalKey]) && is_array($portals[$rawPortalKey])) {
        return $rawPortalKey;
    }

    foreach ($portals as $portalKey => $portalConfig) {
        if (!is_array($portalConfig)) {
            continue;
        }

        $subdomain = trim((string)($portalConfig['subdomain'] ?? ''));
        if ($subdomain !== '' && $subdomain === $rawPortalKey) {
            return (string)$portalKey;
        }
    }

    return '';
}

function yandex_forms_resolve_portal_key(?array $payload = null): string
{
    $config = yandex_forms_config();
    $portalParamName = trim((string)($config['portal_param_name'] ?? 'amo_portal'));
    $candidates = [];

    if ($payload !== null) {
        $queryParams = yandex_forms_extract_query_params($payload);
        if ($portalParamName !== '' && isset($queryParams[$portalParamName])) {
            $candidates[] = (string)$queryParams[$portalParamName];
        }

        if ($portalParamName !== '' && isset($payload[$portalParamName])) {
            $candidates[] = (string)$payload[$portalParamName];
        }
    }

    if ($portalParamName !== '' && isset($_GET[$portalParamName])) {
        $candidates[] = (string)$_GET[$portalParamName];
    }

    foreach ($candidates as $candidate) {
        $normalizedPortalKey = yandex_forms_normalize_portal_key($candidate);
        if ($normalizedPortalKey !== '') {
            return $normalizedPortalKey;
        }
    }

    return 'directorchinatutorru';
}

function yandex_forms_portal_config(?string $portalKey = null, ?array $payload = null): array
{
    $resolvedPortalKey = $portalKey ?? yandex_forms_resolve_portal_key($payload);
    $portals = get_config('amocrm_portals');
    $portalConfig = is_array($portals) ? ($portals[$resolvedPortalKey] ?? null) : null;

    if (!is_array($portalConfig)) {
        throw new RuntimeException('Не найдена конфигурация amoCRM-портала для ключа ' . $resolvedPortalKey . '.');
    }

    $portalConfig['portal_key'] = $resolvedPortalKey;
    return $portalConfig;
}

function yandex_forms_portal_tokens_path(array $portalConfig): string
{
    $tokensFile = trim((string)($portalConfig['tokens_file'] ?? ''));
    if ($tokensFile === '') {
        throw new RuntimeException('Не задан tokens_file для портала ' . ($portalConfig['portal_key'] ?? 'unknown') . '.');
    }

    if ($tokensFile[0] !== '/') {
        $tokensFile = __DIR__ . '/' . ltrim($tokensFile, '/');
    }

    return $tokensFile;
}

function yandex_forms_portal_tokens(array $portalConfig): array
{
    $staticAccessToken = trim((string)($portalConfig['access_token'] ?? ''));
    if ($staticAccessToken !== '') {
        return [
            'access_token' => $staticAccessToken,
            'time' => time(),
        ];
    }

    $tokensFile = yandex_forms_portal_tokens_path($portalConfig);
    if (!is_file($tokensFile)) {
        throw new RuntimeException('Не найден файл токенов amoCRM: ' . basename($tokensFile) . '.');
    }

    $decoded = json_decode((string)file_get_contents($tokensFile), true);
    if (!is_array($decoded)) {
        throw new RuntimeException('Файл токенов amoCRM содержит некорректный JSON: ' . basename($tokensFile) . '.');
    }

    return $decoded;
}

function yandex_forms_refresh_portal_tokens(array $portalConfig, array $tokens): array
{
    $clientId = trim((string)($portalConfig['client_id'] ?? ''));
    $clientSecret = trim((string)($portalConfig['client_secret'] ?? ''));
    $redirectUri = trim((string)($portalConfig['redirect_uri'] ?? ''));
    $subdomain = trim((string)($portalConfig['subdomain'] ?? ''));
    $refreshToken = trim((string)($tokens['refresh_token'] ?? ''));

    if ($clientId === '' || $clientSecret === '' || $redirectUri === '' || $subdomain === '' || $refreshToken === '') {
        throw new RuntimeException('Для портала ' . ($portalConfig['portal_key'] ?? 'unknown') . ' не хватает amoCRM client_id/client_secret/redirect_uri/subdomain/refresh_token.');
    }

    $payload = [
        'client_id' => $clientId,
        'client_secret' => $clientSecret,
        'grant_type' => 'refresh_token',
        'refresh_token' => $refreshToken,
        'redirect_uri' => $redirectUri,
    ];

    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => 'https://' . $subdomain . '.amocrm.ru/oauth2/access_token',
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_USERAGENT => 'amoCRM-oAuth-client/1.0',
        CURLOPT_HTTPHEADER => ['Content-Type: application/json'],
        CURLOPT_CUSTOMREQUEST => 'POST',
        CURLOPT_POSTFIELDS => json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        CURLOPT_SSL_VERIFYPEER => true,
        CURLOPT_SSL_VERIFYHOST => 2,
    ]);

    $response = curl_exec($ch);
    $httpCode = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlError = curl_error($ch);
    curl_close($ch);

    if ($curlError !== '') {
        throw new RuntimeException('Ошибка обновления токена amoCRM: ' . $curlError);
    }

    if ($httpCode < 200 || $httpCode > 204) {
        throw new RuntimeException('amoCRM вернул HTTP ' . $httpCode . ' при обновлении токена: ' . mb_substr((string)$response, 0, 1000, 'UTF-8'));
    }

    $decoded = json_decode((string)$response, true);
    if (!is_array($decoded) || trim((string)($decoded['access_token'] ?? '')) === '') {
        throw new RuntimeException('amoCRM вернул некорректный ответ при обновлении токена.');
    }

    $decoded['time'] = time();
    file_put_contents(
        yandex_forms_portal_tokens_path($portalConfig),
        json_encode($decoded, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES)
    );

    return $decoded;
}

function yandex_forms_portal_access_data(array $portalConfig): array
{
    $tokens = yandex_forms_portal_tokens($portalConfig);
    if (trim((string)($portalConfig['access_token'] ?? '')) !== '') {
        return $tokens;
    }

    $tokenTime = (int)($tokens['time'] ?? 0);

    if ($tokenTime <= 0 || time() - $tokenTime > 82800) {
        $tokens = yandex_forms_refresh_portal_tokens($portalConfig, $tokens);
    }

    return $tokens;
}

function yandex_forms_amo_patch_lead_custom_field(array $portalConfig, array $accessData, int $leadId, int $fieldId, string $value, bool $append = false): void
{
    $subdomain = trim((string)($portalConfig['subdomain'] ?? ''));
    $accessToken = trim((string)($accessData['access_token'] ?? ''));

    if ($subdomain === '' || $accessToken === '') {
        throw new RuntimeException('Не хватает subdomain/access_token для обновления amoCRM по порталу ' . ($portalConfig['portal_key'] ?? 'unknown') . '.');
    }

    if ($append) {
        $existingValues = [];
        $leadResponse = yandex_forms_amo_get_lead($portalConfig, $accessData, $leadId);
        foreach ($leadResponse['custom_fields_values'] ?? [] as $field) {
            if ((int)($field['field_id'] ?? 0) !== $fieldId) {
                continue;
            }
            foreach ($field['values'] ?? [] as $fieldValue) {
                $existingValue = trim((string)($fieldValue['value'] ?? ''));
                if ($existingValue !== '' && !in_array($existingValue, $existingValues, true)) {
                    $existingValues[] = $existingValue;
                }
            }
        }
        if (!in_array($value, $existingValues, true)) {
            $existingValues[] = $value;
        }
        $value = implode("\n", $existingValues);
    }

    $payload = [
        'custom_fields_values' => [
            [
                'field_id' => $fieldId,
                'values' => [['value' => $value]],
            ],
        ],
    ];

    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => 'https://' . $subdomain . '.amocrm.ru/api/v4/leads/' . $leadId,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_CUSTOMREQUEST => 'PATCH',
        CURLOPT_HTTPHEADER => [
            'Authorization: Bearer ' . $accessToken,
            'Content-Type: application/json',
        ],
        CURLOPT_POSTFIELDS => json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        CURLOPT_SSL_VERIFYPEER => true,
        CURLOPT_SSL_VERIFYHOST => 2,
    ]);

    $response = curl_exec($ch);
    $httpCode = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlError = curl_error($ch);
    curl_close($ch);

    if ($curlError !== '') {
        throw new RuntimeException('Ошибка обновления сделки amoCRM: ' . $curlError);
    }

    if ($httpCode < 200 || $httpCode > 204) {
        throw new RuntimeException('amoCRM вернул HTTP ' . $httpCode . ' при обновлении сделки: ' . mb_substr((string)$response, 0, 1000, 'UTF-8'));
    }
}

function yandex_forms_amo_get_lead(array $portalConfig, array $accessData, int $leadId): array
{
    $subdomain = trim((string)($portalConfig['subdomain'] ?? ''));
    $accessToken = trim((string)($accessData['access_token'] ?? ''));

    if ($subdomain === '' || $accessToken === '') {
        throw new RuntimeException('Не хватает subdomain/access_token для получения сделки amoCRM.');
    }

    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => 'https://' . $subdomain . '.amocrm.ru/api/v4/leads/' . $leadId,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_CUSTOMREQUEST => 'GET',
        CURLOPT_HTTPHEADER => [
            'Authorization: Bearer ' . $accessToken,
            'Content-Type: application/json',
        ],
        CURLOPT_SSL_VERIFYPEER => true,
        CURLOPT_SSL_VERIFYHOST => 2,
    ]);

    $response = curl_exec($ch);
    $httpCode = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlError = curl_error($ch);
    curl_close($ch);

    if ($curlError !== '') {
        throw new RuntimeException('Ошибка получения сделки amoCRM: ' . $curlError);
    }

    if ($httpCode < 200 || $httpCode > 204) {
        throw new RuntimeException('amoCRM вернул HTTP ' . $httpCode . ' при получении сделки: ' . mb_substr((string)$response, 0, 1000, 'UTF-8'));
    }

    $decoded = json_decode((string)$response, true);
    if (!is_array($decoded)) {
        throw new RuntimeException('amoCRM вернул некорректные данные сделки.');
    }

    return $decoded;
}

function yandex_forms_amo_create_result_task(
    array $portalConfig,
    array $accessData,
    int $leadId,
    string $answerUrl
): int {
    $config = yandex_forms_config();
    if (($config['task_enabled'] ?? true) !== true) {
        return 0;
    }

    $lead = yandex_forms_amo_get_lead($portalConfig, $accessData, $leadId);
    $responsibleUserId = (int)($lead['responsible_user_id'] ?? 0);
    if ($responsibleUserId <= 0) {
        throw new RuntimeException('У сделки не определен ответственный пользователь amoCRM.');
    }

    $dueHours = max(1, (int)($config['task_due_hours'] ?? 24));
    $taskText = trim((string)($config['task_text'] ?? 'Проверить ответ Яндекс.Формы'));
    if ($answerUrl !== '') {
        $taskText .= ': ' . $answerUrl;
    }

    $payload = [[
        'text' => $taskText,
        'complete_till' => time() + ($dueHours * 3600),
        'entity_id' => $leadId,
        'entity_type' => 'leads',
        'responsible_user_id' => $responsibleUserId,
    ]];

    $subdomain = trim((string)($portalConfig['subdomain'] ?? ''));
    $accessToken = trim((string)($accessData['access_token'] ?? ''));
    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => 'https://' . $subdomain . '.amocrm.ru/api/v4/tasks',
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_CUSTOMREQUEST => 'POST',
        CURLOPT_HTTPHEADER => [
            'Authorization: Bearer ' . $accessToken,
            'Content-Type: application/json',
        ],
        CURLOPT_POSTFIELDS => json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        CURLOPT_SSL_VERIFYPEER => true,
        CURLOPT_SSL_VERIFYHOST => 2,
    ]);

    $response = curl_exec($ch);
    $httpCode = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlError = curl_error($ch);
    curl_close($ch);

    if ($curlError !== '') {
        throw new RuntimeException('Ошибка создания задачи amoCRM: ' . $curlError);
    }

    if ($httpCode < 200 || $httpCode > 204) {
        throw new RuntimeException('amoCRM вернул HTTP ' . $httpCode . ' при создании задачи: ' . mb_substr((string)$response, 0, 1000, 'UTF-8'));
    }

    $decoded = json_decode((string)$response, true);
    return (int)($decoded['_embedded']['tasks'][0]['id'] ?? 0);
}

function yandex_forms_storage_dir(): string
{
    $config = yandex_forms_config();
    $dir = $config['storage_dir'] ?? (__DIR__ . '/data/yandex_forms');

    if (!is_dir($dir)) {
        @mkdir($dir, 0755, true);
    }

    return $dir;
}

function yandex_forms_results_dir(): string
{
    $dir = yandex_forms_storage_dir() . '/results';

    if (!is_dir($dir)) {
        @mkdir($dir, 0755, true);
    }

    return $dir;
}

function yandex_forms_delivery_dir(): string
{
    $dir = yandex_forms_storage_dir() . '/deliveries';

    if (!is_dir($dir)) {
        @mkdir($dir, 0755, true);
    }

    return $dir;
}

function yandex_forms_json_response(array $payload, int $status = 200): void
{
    http_response_code($status);
    header('Content-Type: application/json; charset=utf-8');
    header('Access-Control-Allow-Origin: *');
    echo json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
}

function yandex_forms_parse_request_body(): array
{
    $rawInput = file_get_contents('php://input');
    if (!is_string($rawInput) || trim($rawInput) === '') {
        return yandex_forms_enrich_payload_with_headers($_POST);
    }

    $decoded = json_decode($rawInput, true);
    if (is_array($decoded)) {
        return yandex_forms_enrich_payload_with_headers($decoded);
    }

    $trimmedRawInput = trim($rawInput);

    if ($trimmedRawInput !== '' && $trimmedRawInput[0] === '{' && substr($trimmedRawInput, -1) === '}') {
        $normalizedJson = preg_replace("/'([^'\\\\]*(?:\\\\.[^'\\\\]*)*)'/", '"$1"', $trimmedRawInput);
        $normalizedJson = is_string($normalizedJson) ? preg_replace('/([{,]\s*)([A-Za-z0-9_\-]+)\s*:/', '$1"$2":', $normalizedJson) : null;
        $decodedPseudoJson = is_string($normalizedJson) ? json_decode($normalizedJson, true) : null;

        if (is_array($decodedPseudoJson)) {
            return yandex_forms_enrich_payload_with_headers($decodedPseudoJson);
        }
    }

    parse_str($rawInput, $parsed);
    if (is_array($parsed) && !empty($parsed)) {
        return yandex_forms_enrich_payload_with_headers($parsed);
    }

    if (preg_match('/^\d+$/', $trimmedRawInput) === 1) {
        return yandex_forms_enrich_payload_with_headers([
            'answer_id' => (int)$trimmedRawInput,
        ]);
    }

    $fallbackPayload = $_POST;
    if (empty($fallbackPayload) && !empty($_REQUEST)) {
        $fallbackPayload = $_REQUEST;
    }

    return yandex_forms_enrich_payload_with_headers(is_array($fallbackPayload) ? $fallbackPayload : []);
}

function yandex_forms_enrich_payload_with_headers(array $payload): array
{
    if (empty($payload['query_params']) && !empty($_GET)) {
        $payload['query_params'] = $_GET;
    }

    if ((int)($payload['answer_id'] ?? 0) <= 0) {
        $headerAnswerId = yandex_forms_get_header_value([
            'HTTP_X_FORM_ANSWER_ID',
            'X_FORM_ANSWER_ID',
        ], 'x-form-answer-id');

        if ($headerAnswerId !== '' && preg_match('/^\d+$/', $headerAnswerId) === 1) {
            $payload['answer_id'] = (int)$headerAnswerId;
        }
    }

    if (trim((string)($payload['survey_id'] ?? '')) === '') {
        $headerSurveyId = yandex_forms_get_header_value([
            'HTTP_X_FORM_ID',
            'X_FORM_ID',
        ], 'x-form-id');

        if ($headerSurveyId !== '') {
            $payload['survey_id'] = $headerSurveyId;
        }
    }

    return $payload;
}

function yandex_forms_get_header_value(array $serverKeys, string $headerNameLower): string
{
    foreach ($serverKeys as $key) {
        $value = trim((string)($_SERVER[$key] ?? ''));
        if ($value !== '') {
            return $value;
        }
    }

    if (function_exists('getallheaders')) {
        foreach ((array)getallheaders() as $headerName => $headerValue) {
            if (mb_strtolower((string)$headerName, 'UTF-8') === $headerNameLower) {
                $value = trim((string)$headerValue);
                if ($value !== '') {
                    return $value;
                }
            }
        }
    }

    return '';
}

function yandex_forms_get_delivery_id(): string
{
    return yandex_forms_get_header_value([
        'HTTP_X_DELIVERY_ID',
        'X_DELIVERY_ID',
    ], 'x-delivery-id');
}

function yandex_forms_delivery_already_processed(string $deliveryId): bool
{
    $deliveryId = preg_replace('/[^a-zA-Z0-9\-_]/', '', $deliveryId);
    if ($deliveryId === '') {
        return false;
    }

    $filePath = yandex_forms_delivery_dir() . '/' . $deliveryId . '.lock';
    return is_file($filePath);
}

function yandex_forms_mark_delivery_processed(string $deliveryId, array $meta = []): void
{
    $deliveryId = preg_replace('/[^a-zA-Z0-9\-_]/', '', $deliveryId);
    if ($deliveryId === '') {
        return;
    }

    $filePath = yandex_forms_delivery_dir() . '/' . $deliveryId . '.lock';
    file_put_contents($filePath, json_encode([
        'processed_at' => date(DATE_ATOM),
        'meta' => $meta,
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT));
}

function yandex_forms_request(string $method, string $path, array $query = [], ?array $body = null): array
{
    $config = yandex_forms_config();
    $token = trim((string)($config['token'] ?? ''));
    $orgId = trim((string)($config['org_id'] ?? ''));
    $apiBaseUrl = rtrim((string)($config['api_base_url'] ?? 'https://api.forms.yandex.net/v1'), '/');

    if ($token === '') {
        throw new RuntimeException('Не задан YANDEX_FORMS_TOKEN.');
    }

    $url = $apiBaseUrl . '/' . ltrim($path, '/');
    if (!empty($query)) {
        $url .= '?' . http_build_query($query);
    }

    $headers = [
        'Authorization: OAuth ' . $token,
        'Accept: application/json',
    ];

    if ($orgId !== '') {
        $headers[] = 'X-Org-Id: ' . $orgId;
    }

    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => $url,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_CUSTOMREQUEST => strtoupper($method),
        CURLOPT_HTTPHEADER => $headers,
        CURLOPT_TIMEOUT => 30,
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_SSL_VERIFYPEER => true,
    ]);

    if ($body !== null) {
        $jsonBody = json_encode($body, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        $headers[] = 'Content-Type: application/json';
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $jsonBody);
    }

    $response = curl_exec($ch);
    $httpCode = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlError = curl_error($ch);
    curl_close($ch);

    if ($curlError !== '') {
        throw new RuntimeException('Ошибка запроса к API Яндекс Форм: ' . $curlError);
    }

    if ($httpCode < 200 || $httpCode >= 300) {
        throw new RuntimeException('API Яндекс Форм вернул HTTP ' . $httpCode . ': ' . mb_substr((string)$response, 0, 1000, 'UTF-8'));
    }

    $decoded = json_decode((string)$response, true);
    if (!is_array($decoded)) {
        throw new RuntimeException('API Яндекс Форм вернул некорректный JSON.');
    }

    return $decoded;
}

function yandex_forms_extract_forms_from_response(array $response): array
{
    foreach (['items', 'surveys', 'forms', 'results', 'result'] as $key) {
        if (isset($response[$key]) && is_array($response[$key])) {
            return $response[$key];
        }
    }

    if (isset($response[0]) && is_array($response[0])) {
        return $response;
    }

    return [];
}

function yandex_forms_form_public_url(array $form): string
{
    foreach (['public_url', 'publicUrl', 'share_url', 'shareUrl', 'url', 'public_link', 'publicLink'] as $field) {
        $value = trim((string)($form[$field] ?? ''));
        if ($value !== '') {
            return $value;
        }
    }

    $formId = trim((string)($form['id'] ?? $form['survey_id'] ?? ''));
    if ($formId !== '') {
        return 'https://forms.yandex.ru/u/' . rawurlencode($formId) . '/';
    }

    return '';
}

function yandex_forms_normalize_form(array $form, int $leadId, ?string $leadParamNameOverride = null, ?string $portalKey = null): ?array
{
    $config = yandex_forms_config();
    $leadParamName = trim((string)($leadParamNameOverride !== null && $leadParamNameOverride !== '' ? $leadParamNameOverride : ($config['lead_param_name'] ?? 'amo_lead_id')));
    $portalParamName = trim((string)($config['portal_param_name'] ?? 'amo_portal'));
    $formId = trim((string)($form['id'] ?? $form['survey_id'] ?? $form['slug'] ?? ''));
    $name = trim((string)($form['name'] ?? $form['title'] ?? $form['slug'] ?? $formId));
    $publicUrl = yandex_forms_form_public_url($form);

    if ($formId === '' || $publicUrl === '') {
        return null;
    }

    $query = [
        $leadParamName => (string)$leadId,
    ];

    if ($portalParamName !== '' && $portalKey !== null && trim($portalKey) !== '') {
        $query[$portalParamName] = trim($portalKey);
    }

    $separator = strpos($publicUrl, '?') === false ? '?' : '&';
    $generatedLink = $publicUrl . $separator . http_build_query($query);

    return [
        'id' => $formId,
        'name' => $name !== '' ? $name : $formId,
        'public_url' => $publicUrl,
        'generated_link' => $generatedLink,
    ];
}

function yandex_forms_list_forms_for_lead(int $leadId, ?string $leadParamNameOverride = null, ?string $portalKey = null): array
{
    $response = yandex_forms_request('GET', '/surveys');
    $forms = yandex_forms_extract_forms_from_response($response);
    $result = [];

    foreach ($forms as $form) {
        if (!is_array($form)) {
            continue;
        }

        $normalized = yandex_forms_normalize_form($form, $leadId, $leadParamNameOverride, $portalKey);
        if ($normalized !== null) {
            $result[] = $normalized;
        }
    }

    usort($result, static function (array $a, array $b): int {
        return strcasecmp($a['name'], $b['name']);
    });

    return $result;
}

function yandex_forms_fetch_answer(?int $answerId = null, ?string $answerKey = null): array
{
    $query = [];

    if ($answerId !== null && $answerId > 0) {
        $query['answer_id'] = $answerId;
    } elseif ($answerKey !== null && trim($answerKey) !== '') {
        $query['answer_key'] = trim($answerKey);
    } else {
        throw new RuntimeException('Для получения ответа Яндекс.Форм нужен answer_id или answer_key.');
    }

    return yandex_forms_request('GET', '/answers', $query);
}

function yandex_forms_extract_lead_id_from_payload(array $payload): int
{
    $config = yandex_forms_config();
    $leadParamName = trim((string)($config['lead_param_name'] ?? 'amo_lead_id'));
    $queryParams = yandex_forms_extract_query_params($payload);

    if (is_array($queryParams) && isset($queryParams[$leadParamName])) {
        $leadId = (int)$queryParams[$leadParamName];
        if ($leadId > 0) {
            return $leadId;
        }
    }

    if (isset($payload[$leadParamName])) {
        $leadId = (int)$payload[$leadParamName];
        if ($leadId > 0) {
            return $leadId;
        }
    }

    $surveyItems = $payload['survey']['items'] ?? $payload['items'] ?? null;
    if (is_array($surveyItems)) {
        foreach ($surveyItems as $item) {
            if (!is_array($item)) {
                continue;
            }

            $slug = trim((string)($item['slug'] ?? ''));
            if ($slug !== $leadParamName) {
                continue;
            }

            $value = $item['value'] ?? null;
            if (is_array($value)) {
                $value = $value['value'] ?? reset($value);
            }

            $leadId = (int)$value;
            if ($leadId > 0) {
                return $leadId;
            }
        }
    }

    $answerData = $payload['data'] ?? null;
    if (is_array($answerData)) {
        foreach ($answerData as $item) {
            if (!is_array($item)) {
                continue;
            }

            $itemId = trim((string)($item['id'] ?? ''));
            if ($itemId !== $leadParamName) {
                continue;
            }

            $value = $item['value'] ?? null;
            if (is_array($value)) {
                $value = $value['value'] ?? reset($value);
            }

            $leadId = (int)$value;
            if ($leadId > 0) {
                return $leadId;
            }
        }
    }

    if (isset($payload['answers_json']) && is_string($payload['answers_json']) && trim($payload['answers_json']) !== '') {
        $decodedAnswers = json_decode($payload['answers_json'], true);
        if (is_array($decodedAnswers) && isset($decodedAnswers[$leadParamName])) {
            $answerEntry = $decodedAnswers[$leadParamName];
            $value = is_array($answerEntry) ? ($answerEntry['value'] ?? null) : $answerEntry;
            $leadId = (int)$value;
            if ($leadId > 0) {
                return $leadId;
            }
        }
    }

    return 0;
}

function yandex_forms_extract_answer_rows(array $answer, array $payload = []): array
{
    $rows = [];
    $surveyItems = $answer['survey']['items'] ?? [];

    if (is_array($surveyItems)) {
        foreach ($surveyItems as $item) {
            if (!is_array($item)) {
                continue;
            }

            $label = trim((string)($item['label'] ?? $item['slug'] ?? $item['id'] ?? 'Поле'));
            $value = $item['value'] ?? '';

            if (is_array($value)) {
                $parts = [];
                foreach ($value as $subValue) {
                    if (is_array($subValue)) {
                        $parts[] = (string)($subValue['label'] ?? $subValue['text'] ?? $subValue['value'] ?? json_encode($subValue, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
                    } else {
                        $parts[] = (string)$subValue;
                    }
                }
                $value = implode(', ', array_filter($parts, static fn($part) => trim($part) !== ''));
            } elseif (is_bool($value)) {
                $value = $value ? 'Да' : 'Нет';
            } elseif ($value === null) {
                $value = '';
            } else {
                $value = (string)$value;
            }

            $rows[] = [
                'label' => $label,
                'value' => trim($value),
            ];
        }
    }

    if (empty($rows) && isset($payload['answers_json'])) {
        $answersJson = $payload['answers_json'];
        if (is_string($answersJson) && trim($answersJson) !== '') {
            $decoded = json_decode($answersJson, true);
            if (is_array($decoded)) {
                foreach ($decoded as $key => $item) {
                    if (!is_array($item)) {
                        continue;
                    }

                    $value = $item['value'] ?? '';
                    if (is_array($value)) {
                        $value = json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
                    } elseif (is_bool($value)) {
                        $value = $value ? 'Да' : 'Нет';
                    } elseif ($value === null) {
                        $value = '';
                    }

                    $rows[] = [
                        'label' => (string)$key,
                        'value' => trim((string)$value),
                    ];
                }
            }
        }
    }

    return $rows;
}

function yandex_forms_build_snapshot(array $incomingPayload): array
{
    $leadId = yandex_forms_extract_lead_id_from_payload($incomingPayload);
    $answerId = (int)($incomingPayload['answer_id'] ?? 0);
    $answerKey = trim((string)($incomingPayload['answer_key'] ?? ''));
    $portalKey = yandex_forms_resolve_portal_key($incomingPayload);

    if ($answerId <= 0 && $answerKey === '') {
        throw new RuntimeException('В webhook Яндекс.Форм должен передаваться answer_id или answer_key.');
    }

    $answer = yandex_forms_fetch_answer($answerId > 0 ? $answerId : null, $answerKey !== '' ? $answerKey : null);

    if ($leadId <= 0) {
        $leadId = yandex_forms_extract_lead_id_from_payload($answer);
    }

    if ($leadId <= 0) {
        throw new RuntimeException('Не удалось определить lead_id для обновления amoCRM.');
    }

    $rows = yandex_forms_extract_answer_rows($answer, $incomingPayload);

    $survey = $answer['survey'] ?? [];
    $surveyName = trim((string)($survey['name'] ?? $incomingPayload['survey_name'] ?? 'Анкета'));
    $surveyId = trim((string)($survey['id'] ?? $incomingPayload['survey_id'] ?? ''));
    $createdAt = trim((string)($answer['created'] ?? $incomingPayload['answer_created'] ?? date(DATE_ATOM)));

    return [
        'lead_id' => $leadId,
        'portal_key' => $portalKey,
        'answer_id' => $answerId,
        'answer_key' => $answerKey,
        'survey_id' => $surveyId,
        'survey_name' => $surveyName !== '' ? $surveyName : 'Анкета',
        'created_at' => $createdAt,
        'rows' => $rows,
        'raw_payload' => $incomingPayload,
        'raw_answer' => $answer,
    ];
}

function yandex_forms_answer_url(array $snapshot): string
{
    $surveyId = trim((string)($snapshot['survey_id'] ?? ''));
    $answerId = (int)($snapshot['answer_id'] ?? 0);

    if ($surveyId === '' || $answerId <= 0) {
        return '';
    }

    return 'https://forms.yandex.ru/cloud/admin/'
        . rawurlencode($surveyId)
        . '/answers/'
        . rawurlencode((string)$answerId);
}

function yandex_forms_store_snapshot(array $snapshot): string
{
    $seed = implode('|', [
        (string)($snapshot['lead_id'] ?? 0),
        (string)($snapshot['survey_id'] ?? ''),
        (string)($snapshot['answer_id'] ?? 0),
        (string)($snapshot['created_at'] ?? ''),
    ]);
    $publicId = substr(sha1($seed !== '|||' ? $seed : uniqid('yform_', true)), 0, 24);
    $targetFile = yandex_forms_results_dir() . '/' . $publicId . '.dat';

    file_put_contents($targetFile, json_encode($snapshot, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT));

    return $publicId;
}

function yandex_forms_load_snapshot(string $publicId): ?array
{
    $publicId = preg_replace('/[^a-zA-Z0-9]/', '', $publicId);
    if ($publicId === '') {
        return null;
    }

    $targetFile = yandex_forms_results_dir() . '/' . $publicId . '.dat';
    if (!is_file($targetFile)) {
        return null;
    }

    $decoded = json_decode((string)file_get_contents($targetFile), true);
    return is_array($decoded) ? $decoded : null;
}

function yandex_forms_public_result_url(string $publicId): string
{
    $config = yandex_forms_config();
    $baseUrl = rtrim((string)($config['public_base_url'] ?? 'https://srm.chinatutor.ru'), '/');
    return $baseUrl . '/yandex_forms_result.php?id=' . rawurlencode($publicId);
}

function yandex_forms_update_amo_result_link(int $leadId, string $publicUrl, ?string $portalKey = null): void
{
    if ($leadId <= 0) {
        throw new RuntimeException('Не удалось определить lead_id для обновления amoCRM.');
    }

    $portalConfig = yandex_forms_portal_config($portalKey);
    $fieldId = (int)($portalConfig['results_field_id'] ?? 0);
    $historyFieldId = (int)($portalConfig['results_history_field_id'] ?? 0);
    if ($fieldId <= 0) {
        throw new RuntimeException('Не задан results_field_id для портала ' . ($portalConfig['portal_key'] ?? 'unknown') . '.');
    }

    $accessData = yandex_forms_portal_access_data($portalConfig);
    yandex_forms_amo_patch_lead_custom_field($portalConfig, $accessData, $leadId, $fieldId, $publicUrl);
    if ($historyFieldId > 0 && $historyFieldId !== $fieldId) {
        yandex_forms_amo_patch_lead_custom_field($portalConfig, $accessData, $leadId, $historyFieldId, $publicUrl, true);
    }
}
