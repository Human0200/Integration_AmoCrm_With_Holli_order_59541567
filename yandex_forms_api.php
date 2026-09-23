<?php

declare(strict_types=1);

require_once __DIR__ . '/yandex_forms_common.php';

if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') {
    header('Access-Control-Allow-Origin: *');
    header('Access-Control-Allow-Methods: GET, OPTIONS');
    header('Access-Control-Allow-Headers: Content-Type');
    http_response_code(204);
    exit;
}

$action = trim((string)($_GET['action'] ?? 'forms'));

try {
    if ($action !== 'forms') {
        yandex_forms_json_response([
            'success' => false,
            'error' => 'Неизвестное действие.',
        ], 400);
        exit;
    }

    $leadId = (int)($_GET['lead_id'] ?? 0);
    if ($leadId <= 0) {
        yandex_forms_json_response([
            'success' => false,
            'error' => 'Не передан lead_id.',
        ], 400);
        exit;
    }

    $leadParamName = trim((string)($_GET['lead_param_name'] ?? ''));
    $portalKey = yandex_forms_resolve_portal_key([
        'query_params' => $_GET,
    ]);
    $forms = yandex_forms_list_forms_for_lead($leadId, $leadParamName !== '' ? $leadParamName : null, $portalKey);
    $config = yandex_forms_config();

    yandex_forms_json_response([
        'success' => true,
        'lead_id' => $leadId,
        'portal_key' => $portalKey,
        'lead_param_name' => $leadParamName !== '' ? $leadParamName : ($config['lead_param_name'] ?? 'amo_lead_id'),
        'portal_param_name' => $config['portal_param_name'] ?? 'amo_portal',
        'forms' => $forms,
        'setup' => [
            'webhook_url' => rtrim((string)($config['public_base_url'] ?? 'https://srm.chinatutor.ru'), '/') . '/yandex_forms_webhook.php',
            'hidden_field_hint' => $leadParamName !== '' ? $leadParamName : ($config['lead_param_name'] ?? 'amo_lead_id'),
        ],
    ]);
} catch (Throwable $e) {
    log_error('Ошибка API Яндекс.Форм для виджета', [
        'action' => $action,
        'error' => $e->getMessage(),
    ], 'yandex_forms_api.php');

    yandex_forms_json_response([
        'success' => false,
        'error' => $e->getMessage(),
    ], 500);
}
