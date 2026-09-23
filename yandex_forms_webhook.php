<?php

declare(strict_types=1);

require_once __DIR__ . '/yandex_forms_common.php';

if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') {
    header('Access-Control-Allow-Origin: *');
    header('Access-Control-Allow-Methods: POST, OPTIONS');
    header('Access-Control-Allow-Headers: Content-Type');
    http_response_code(204);
    exit;
}

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    yandex_forms_json_response([
        'success' => false,
        'error' => 'Используйте POST.',
    ], 405);
    exit;
}

try {
    $deliveryId = yandex_forms_get_delivery_id();
    if ($deliveryId !== '' && yandex_forms_delivery_already_processed($deliveryId)) {
        log_info('Повторный webhook Яндекс.Форм пропущен по x-delivery-id', [
            'delivery_id' => $deliveryId,
        ], 'yandex_forms_webhook.php');

        yandex_forms_json_response([
            'success' => true,
            'duplicate' => true,
            'delivery_id' => $deliveryId,
        ], 200);
        exit;
    }

    $payload = yandex_forms_parse_request_body();
    log_info('Получен webhook Яндекс.Форм', [
        'payload_keys' => array_keys($payload),
        'delivery_id' => $deliveryId,
    ], 'yandex_forms_webhook.php');

    $snapshot = yandex_forms_build_snapshot($payload);
    $publicId = yandex_forms_store_snapshot($snapshot);
    $publicUrl = yandex_forms_public_result_url($publicId);
    $answerUrl = yandex_forms_answer_url($snapshot);
    $amoResultUrl = $answerUrl !== '' ? $answerUrl : $publicUrl;

    yandex_forms_update_amo_result_link(
        (int)($snapshot['lead_id'] ?? 0),
        $amoResultUrl,
        (string)($snapshot['portal_key'] ?? '')
    );
    $portalConfig = yandex_forms_portal_config((string)($snapshot['portal_key'] ?? ''));
    $accessData = yandex_forms_portal_access_data($portalConfig);
    $taskId = yandex_forms_amo_create_result_task(
        $portalConfig,
        $accessData,
        (int)($snapshot['lead_id'] ?? 0),
        $amoResultUrl
    );
    if ($deliveryId !== '') {
        yandex_forms_mark_delivery_processed($deliveryId, [
            'lead_id' => $snapshot['lead_id'] ?? 0,
            'answer_id' => $snapshot['answer_id'] ?? 0,
            'public_id' => $publicId,
            'answer_url' => $answerUrl,
            'task_id' => $taskId,
        ]);
    }

    log_info('Ответ Яндекс.Форм сохранен и ссылка записана в amoCRM', [
        'lead_id' => $snapshot['lead_id'] ?? 0,
        'answer_id' => $snapshot['answer_id'] ?? 0,
        'public_url' => $publicUrl,
        'answer_url' => $answerUrl,
        'amo_result_url' => $amoResultUrl,
        'task_id' => $taskId,
        'delivery_id' => $deliveryId,
    ], 'yandex_forms_webhook.php');

    yandex_forms_json_response([
        'success' => true,
        'lead_id' => $snapshot['lead_id'] ?? 0,
        'answer_url' => $answerUrl,
        'public_url' => $publicUrl,
        'task_id' => $taskId,
    ], 200);
} catch (Throwable $e) {
    log_error('Ошибка обработки webhook Яндекс.Форм', [
        'error' => $e->getMessage(),
    ], 'yandex_forms_webhook.php');

    yandex_forms_json_response([
        'success' => false,
        'error' => $e->getMessage(),
    ], 500);
}
