<?php

declare(strict_types=1);

require_once __DIR__ . '/config.php';
require_once __DIR__ . '/logger.php';
require_once __DIR__ . '/salebot_amocrm_chat_bridge.php';

header('Content-Type: application/json; charset=utf-8');

if (($_SERVER['REQUEST_METHOD'] ?? '') !== 'POST') {
    http_response_code(405);
    header('Allow: POST');
    echo json_encode(['success' => false, 'error' => 'Method not allowed']);
    exit;
}

$rawBody = file_get_contents('php://input');
$requestPath = parse_url((string)($_SERVER['REQUEST_URI'] ?? ''), PHP_URL_PATH);
$pathScopeId = (string)($_SERVER['PATH_INFO'] ?? $_SERVER['ORIG_PATH_INFO'] ?? '');
if ($pathScopeId === '' && is_string($requestPath)) {
    $scriptName = (string)($_SERVER['SCRIPT_NAME'] ?? '/amo_chat_webhook.php');
    $pathScopeId = preg_replace('~^' . preg_quote($scriptName, '~') . '/?~', '', $requestPath) ?? '';
}
$scopeId = trim((string)($_GET['scope_id'] ?? $pathScopeId));
$scopeId = trim($scopeId, "/ \t\n\r\0\x0B");
$signature = trim((string)($_SERVER['HTTP_X_SIGNATURE'] ?? ''));
$apiKey = trim((string)(getenv('AMO_CHAT_SALEBOT_API_KEY') ?: ''));

if (!is_string($rawBody) || $rawBody === '' || $signature === '' || $apiKey === '') {
    http_response_code(400);
    echo json_encode(['success' => false, 'error' => 'Missing body, signature, or Salebot API key']);
    exit;
}

$result = salebot_amocrm_bridge_handle_amo_webhook($rawBody, $signature, $scopeId, $apiKey);
http_response_code((int)($result['http_status'] ?? 500));
echo json_encode($result, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
