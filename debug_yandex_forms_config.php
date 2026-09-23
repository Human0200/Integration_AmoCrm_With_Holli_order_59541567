<?php

declare(strict_types=1);

require_once __DIR__ . '/config.php';

$token = (string) get_config('yandex_forms.token');

header('Content-Type: application/json; charset=utf-8');
echo json_encode([
    'dir' => __DIR__,
    'env_exists' => file_exists(__DIR__ . '/.env'),
    'env_local_exists' => file_exists(__DIR__ . '/.env.local'),
    'token_present' => $token !== '',
    'token_prefix' => $token !== '' ? substr($token, 0, 8) : '',
    'widget_server_url' => get_config('yandex_forms.widget_server_url'),
], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
