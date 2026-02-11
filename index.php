<?php

require_once __DIR__ . '/logger.php';
require_once __DIR__ . '/config.php';
require_once __DIR__ . '/amo_func.php';

log_info("=== START index.php ===", [], 'index.php');

try {

    // =============================================
    // 1. ОБРАБОТКА OKIDOKI
    // =============================================

    $input = file_get_contents('php://input');
    $oki_data = json_decode($input, true);

    if (is_array($oki_data) && ($oki_data['status'] ?? null) === 'signed') {

        log_info("OKIDOKI webhook received", $oki_data, 'index.php');

        $lead_id = $oki_data['lead_id'] ?? null;
        $fio = $oki_data['extra_fields']['ФИО клиента'] ?? '';
        $email = $oki_data['extra_fields']['E-Mail клиента'] ?? '';

        if ($lead_id) {

            $lead_info = get($subdomain, "/api/v4/leads/$lead_id?with=contacts", $data);
            $contact_id = $lead_info['_embedded']['contacts'][0]['id'] ?? null;

            if ($contact_id) {

                $contact_update = [
                    'name' => $fio,
                    'custom_fields_values' => [
                        [
                            'field_code' => 'EMAIL',
                            'values' => [['value' => $email, 'enum_code' => 'WORK']]
                        ]
                    ]
                ];

                post_or_patch($subdomain, $contact_update, "/api/v4/contacts/$contact_id", $data, 'PATCH');
                log_info("Contact updated from Okidoki", ['contact_id' => $contact_id], 'index.php');
            }
        }

        exit('OK');
    }

    // =============================================
    // 2. ОБРАБОТКА WEBHOOK AMO
    // =============================================

    log_info("Amo webhook received", $_POST, 'index.php');

    $lead_id = null;

    if (isset($_POST["leads"]["add"][0]["id"])) {
        $lead_id = (int) $_POST["leads"]["add"][0]["id"];
    } elseif (isset($_POST["leads"]["status"][0]["id"])) {
        $lead_id = (int) $_POST["leads"]["status"][0]["id"];
    }

    if (!$lead_id) {
        log_warning("Lead ID not found", $_POST, 'index.php');
        exit;
    }

    log_info("Processing lead", ['lead_id' => $lead_id], 'index.php');

    $LEAD = get($subdomain, "/api/v4/leads/$lead_id?with=contacts", $data);

    if (!$LEAD) {
        throw new Exception("Lead not loaded");
    }

    // =============================================
    // 3. СОБИРАЕМ JSON ДЛЯ HOLLYHOP
    // =============================================

    $json = [
        'Status' => 'В наборе',
        'link' => "https://{$subdomain}.amocrm.ru/leads/detail/{$lead_id}",
        'gender' => 'F',
        'amo_lead_id' => $lead_id,
        'amo_subdomain' => $subdomain,
        'useMobileBySystem' => false,
        'useEMailBySystem' => false
    ];

    // кастомные поля сделки
    foreach ($LEAD['custom_fields_values'] ?? [] as $field) {

        switch ($field['field_id']) {
            case 1575217:
                $json['discipline'] = $field['values'][0]['value'] ?? '';
                break;
            case 1576357:
                $json['level'] = $field['values'][0]['value'] ?? '';
                break;
            case 1575221:
                $json['learningType'] = $field['values'][0]['value'] ?? '';
                break;
            case 1575213:
                $json['maturity'] = $field['values'][0]['value'] ?? '';
                break;
            case 1596219:
                $json['officeOrCompanyId'] = $field['values'][0]['value'] ?? '';
                break;
            case 1590693:
                $json['responsible_user'] = $field['values'][0]['value'] ?? '';
                break;
        }
    }

    // =============================================
    // 4. КОНТАКТ
    // =============================================

    $contact_id = $LEAD['_embedded']['contacts'][0]['id'] ?? null;

    if (!$contact_id) {
        log_warning("Lead has no contact", ['lead_id' => $lead_id], 'index.php');
        exit;
    }

    $CONTACT = get($subdomain, "/api/v4/contacts/$contact_id", $data);

    $name_parts = explode(' ', $CONTACT['name'] ?? '');

    $json['firstName'] = $name_parts[0] ?? '';
    $json['lastName']  = $name_parts[1] ?? '';

    foreach ($CONTACT['custom_fields_values'] ?? [] as $field) {
        if ($field['field_code'] === 'PHONE') {
            $json['phone'] = $field['values'][0]['value'] ?? '';
        }
        if ($field['field_code'] === 'EMAIL') {
            $json['email'] = $field['values'][0]['value'] ?? '';
        }
    }

    if (empty($json['firstName'])) {
        log_warning("Student firstName missing, skip creation", $json, 'index.php');
        exit;
    }

    log_info("Prepared JSON for Hollyhop", $json, 'index.php');

    // =============================================
    // 5. ВЫЗОВ add_student.php
    // =============================================

    $ch = curl_init('https://srm.chinatutor.ru/add_student.php');

    curl_setopt_array($ch, [
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => json_encode($json, JSON_UNESCAPED_UNICODE),
        CURLOPT_HTTPHEADER => ['Content-Type: application/json'],
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => 120,
        CURLOPT_SSL_VERIFYPEER => true
    ]);

    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);

    if (curl_errno($ch)) {
        throw new Exception("cURL error: " . curl_error($ch));
    }

    curl_close($ch);

    log_info("Hollyhop response", [
        'http_code' => $httpCode,
        'response' => $response
    ], 'index.php');

    if ($httpCode >= 400) {
        throw new Exception("Hollyhop returned HTTP $httpCode");
    }

    $res = json_decode(trim($response), true);

    if (!isset($res['link'])) {
        log_warning("No link returned from Hollyhop", $res, 'index.php');
        exit;
    }

    // =============================================
    // 6. ОБНОВЛЕНИЕ AMO
    // =============================================

    $leads_data = [
        'id' => $lead_id,
        'custom_fields_values' => [
            [
                'field_id' => 1630807,
                'values' => [
                    ['value' => $res['link']]
                ]
            ]
        ]
    ];

    post_or_patch(
        $subdomain,
        $leads_data,
        "/api/v4/leads/$lead_id",
        $data,
        'PATCH'
    );

    log_info("Amo lead updated with Hollyhop link", [
        'lead_id' => $lead_id,
        'link' => $res['link']
    ], 'index.php');

    log_info("=== END index.php SUCCESS ===", [], 'index.php');
} catch (Throwable $e) {

    log_error("FATAL ERROR", [
        'message' => $e->getMessage(),
        'trace' => $e->getTraceAsString()
    ], 'index.php');
}
