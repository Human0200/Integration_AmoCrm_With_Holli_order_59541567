<?php

declare(strict_types=1);

require_once __DIR__ . '/yandex_forms_common.php';

$publicId = trim((string)($_GET['id'] ?? ''));
$snapshot = yandex_forms_load_snapshot($publicId);

if ($snapshot === null) {
    http_response_code(404);
    header('Content-Type: text/html; charset=utf-8');
    echo '<!doctype html><html lang="ru"><head><meta charset="utf-8"><title>Анкета не найдена</title></head><body><h1>Анкета не найдена</h1></body></html>';
    exit;
}

$surveyName = htmlspecialchars((string)($snapshot['survey_name'] ?? 'Анкета'), ENT_QUOTES, 'UTF-8');
$createdAt = htmlspecialchars((string)($snapshot['created_at'] ?? ''), ENT_QUOTES, 'UTF-8');
$leadId = (int)($snapshot['lead_id'] ?? 0);
$rows = is_array($snapshot['rows'] ?? null) ? $snapshot['rows'] : [];

?><!doctype html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title><?php echo $surveyName; ?></title>
    <style>
        :root {
            --bg: #f5f1e8;
            --paper: #fffdf9;
            --ink: #22201c;
            --muted: #6a655e;
            --line: #ded6c9;
            --accent: #b85c38;
        }
        body {
            margin: 0;
            font-family: Georgia, "Times New Roman", serif;
            background:
                radial-gradient(circle at top left, rgba(184, 92, 56, 0.08), transparent 24rem),
                linear-gradient(180deg, #f7f2e9 0%, #efe6d8 100%);
            color: var(--ink);
        }
        .page {
            max-width: 920px;
            margin: 40px auto;
            padding: 0 20px 40px;
        }
        .card {
            background: var(--paper);
            border: 1px solid var(--line);
            border-radius: 20px;
            box-shadow: 0 24px 60px rgba(34, 32, 28, 0.08);
            overflow: hidden;
        }
        .hero {
            padding: 32px 32px 24px;
            border-bottom: 1px solid var(--line);
        }
        .eyebrow {
            margin: 0 0 10px;
            color: var(--accent);
            font-size: 12px;
            letter-spacing: 0.14em;
            text-transform: uppercase;
        }
        h1 {
            margin: 0;
            font-size: 34px;
            line-height: 1.15;
        }
        .meta {
            margin-top: 14px;
            color: var(--muted);
            font-size: 15px;
        }
        .table {
            width: 100%;
            border-collapse: collapse;
        }
        .table th,
        .table td {
            vertical-align: top;
            padding: 16px 32px;
            border-bottom: 1px solid var(--line);
            text-align: left;
        }
        .table th {
            width: 34%;
            color: var(--muted);
            font-size: 14px;
            font-weight: 600;
        }
        .table td {
            font-size: 16px;
            white-space: pre-wrap;
            word-break: break-word;
        }
        .empty {
            padding: 32px;
            color: var(--muted);
        }
        @media (max-width: 700px) {
            .hero,
            .table th,
            .table td,
            .empty {
                padding-left: 18px;
                padding-right: 18px;
            }
            h1 {
                font-size: 28px;
            }
            .table,
            .table tbody,
            .table tr,
            .table th,
            .table td {
                display: block;
                width: 100%;
            }
            .table th {
                padding-bottom: 6px;
                border-bottom: 0;
            }
            .table td {
                padding-top: 0;
            }
        }
    </style>
</head>
<body>
    <div class="page">
        <div class="card">
            <div class="hero">
                <p class="eyebrow">Результат анкеты</p>
                <h1><?php echo $surveyName; ?></h1>
                <div class="meta">
                    Сделка amoCRM: <?php echo $leadId > 0 ? $leadId : 'не определена'; ?>
                    <?php if ($createdAt !== ''): ?>
                        <br>Дата ответа: <?php echo $createdAt; ?>
                    <?php endif; ?>
                </div>
            </div>

            <?php if (empty($rows)): ?>
                <div class="empty">Ответ сохранён, но распознанных полей для отображения не найдено.</div>
            <?php else: ?>
                <table class="table">
                    <tbody>
                    <?php foreach ($rows as $row): ?>
                        <tr>
                            <th><?php echo htmlspecialchars((string)($row['label'] ?? ''), ENT_QUOTES, 'UTF-8'); ?></th>
                            <td><?php echo htmlspecialchars((string)($row['value'] ?? ''), ENT_QUOTES, 'UTF-8'); ?></td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            <?php endif; ?>
        </div>
    </div>
</body>
</html>
