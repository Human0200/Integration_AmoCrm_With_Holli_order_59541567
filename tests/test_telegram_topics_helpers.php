<?php

declare(strict_types=1);

require_once __DIR__ . '/bootstrap.php';

$sourceFile = tests_project_root() . '/telegram_topics_webhook.php';
tests_load_functions($sourceFile, [
    'telegram_topics_extract_message',
    'telegram_topics_is_getthemes_command',
    'telegram_topics_extract_context',
]);

$update = [
    'update_id' => 1001,
    'message' => [
        'message_id' => 55,
        'message_thread_id' => 77,
        'text' => '/getthemes@help_evrasia_bot',
        'chat' => [
            'id' => -1001234567890,
            'type' => 'supergroup',
            'title' => 'Консультации',
        ],
        'from' => [
            'id' => 753744248,
            'username' => 'manager',
        ],
    ],
];

$message = telegram_topics_extract_message($update);
tests_assert_same('/getthemes@help_evrasia_bot', $message['text'], 'Telegram topics helper should extract message body.');
tests_assert_true(telegram_topics_is_getthemes_command($message), 'Command /getthemes with bot mention should be recognized.');

$context = telegram_topics_extract_context($message);
tests_assert_same(-1001234567890, $context['chat_id'], 'Chat id should be extracted from Telegram message.');
tests_assert_same(77, $context['message_thread_id'], 'message_thread_id should be extracted from Telegram message.');
tests_assert_same('supergroup', $context['chat_type'], 'Chat type should be preserved.');
tests_assert_same('Консультации', $context['chat_title'], 'Chat title should be preserved.');
tests_assert_same(753744248, $context['from_id'], 'Sender id should be preserved.');

tests_assert_true(!telegram_topics_is_getthemes_command(['text' => '/start']), 'Other Telegram commands must be ignored.');
tests_assert_same(null, telegram_topics_extract_message(['callback_query' => []]), 'Updates without message payload should return null.');

echo "OK\n";
