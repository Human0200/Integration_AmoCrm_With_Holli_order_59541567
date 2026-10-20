<?php

declare(strict_types=1);

require_once __DIR__ . '/bootstrap.php';
tests_load_functions(tests_project_root() . '/integrationSalebot.php', [
    'salebot_extract_client_link',
    'salebot_date_to_timestamp',
    'salebot_build_extra_fields',
    'salebot_messenger_fields',
]);

tests_assert_same(
    'https://example.com/client/42',
    salebot_extract_client_link([], ['profile_url' => 'https://example.com/client/42']),
    'A real Salebot client URL should be extracted from variables.'
);
tests_assert_same(
    'https://example.com/from-payload',
    salebot_extract_client_link(['client' => ['url' => 'https://example.com/from-payload']], []),
    'A real client URL should be extracted from the webhook payload.'
);
tests_assert_same(
    null,
    salebot_extract_client_link([], ['profile_url' => 'not-a-url']),
    'Invalid URLs must not be written into amoCRM.'
);

$extraFields = salebot_build_extra_fields(
    [
        'domain' => 'vitaliya_yan',
        'sex' => 1,
        'cmid' => 37,
        'bdate' => '20.5.2000',
    ],
    '1041568516',
    '161257902',
    '15916141',
    null
);
$byId = [];
foreach ($extraFields as $field) {
    $byId[$field['field_id']] = $field['values'][0]['value'];
}
tests_assert_same([], array_values(array_intersect(array_keys($byId), [1641510, 1641512, 1641514, 1641516, 1641518, 1641520])), 'Technical Salebot fields should not be mapped into amoCRM.');
tests_assert_same(salebot_date_to_timestamp('20.5.2000'), $byId[1598451] ?? null, 'Birth date should be converted to a timestamp.');

$telegram = salebot_messenger_fields(['client' => [
    'client_type' => 1,
    'recepient' => 450610122,
    'order_variables' => ['tg_request' => '{"message":{"from":{"id":450610122,"username":"yanchesik"}}}'],
]], ['tg_username' => '@yanchesik']);
tests_assert_same('450610122', $telegram[1621101] ?? null, 'Telegram ID should come from the sender.');
tests_assert_same('@yanchesik', $telegram[1629273] ?? null, 'Telegram username should be mapped.');
tests_assert_true(!isset($telegram[1632097]), 'Do not invent a Telegram phone number.');

$max = salebot_messenger_fields(['client' => [
    'client_type' => 20,
    'recepient' => 45471360,
    'order_variables' => ['tt_request' => '{"chat_id":45471360,"user":{"user_id":140691697}}'],
]], ['tamtam_user_id' => 140691697]);
tests_assert_same('45471360', $max[1630424] ?? null, 'MAX chat ID should be mapped.');
tests_assert_same('140691697', $max[1631985] ?? null, 'MAX user ID should be mapped.');

echo "OK\n";
