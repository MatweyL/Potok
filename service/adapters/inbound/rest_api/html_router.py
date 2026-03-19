from fastapi import APIRouter

router = APIRouter()


@router.get("/")
def index():
    return """
    <!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Поток — Система управления задачами</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet"/>
  <style>
    :root {
      --bg-primary: #0f172a;
      --bg-secondary: #1e293b;
      --bg-card: #334155;
      --text-primary: #f8fafc;
      --text-secondary: #cbd5e1;
      --accent: #6366f1;
      --accent-hover: #818cf8;
      --success: #22c55e;
      --warning: #f59e0b;
      --border-radius: 16px;
      --shadow: 0 10px 40px -10px rgba(0,0,0,0.3);
      --transition: all 0.3s ease;
    }

    * {
      margin: 0;
      padding: 0;
      box-sizing: border-box;
    }

    body {
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
      background: linear-gradient(135deg, var(--bg-primary), #1e1b4b);
      color: var(--text-primary);
      line-height: 1.6;
      min-height: 100vh;
      padding: 2rem 1rem;
    }

    .container {
      max-width: 900px;
      margin: 0 auto;
    }

    /* Header */
    header {
      text-align: center;
      padding: 3rem 1rem;
      animation: fadeInDown 0.6s ease;
    }

    .logo {
      display: inline-flex;
      align-items: center;
      gap: 0.75rem;
      margin-bottom: 1rem;
    }

    .logo-icon {
      width: 56px;
      height: 56px;
      background: linear-gradient(135deg, var(--accent), var(--accent-hover));
      border-radius: 14px;
      display: flex;
      align-items: center;
      justify-content: center;
      font-weight: 700;
      font-size: 1.5rem;
      box-shadow: 0 8px 25px rgba(99, 102, 241, 0.4);
    }

    h1 {
      font-size: 2.5rem;
      font-weight: 700;
      letter-spacing: -0.025em;
      margin-bottom: 0.5rem;
    }

    .subtitle {
      font-size: 1.25rem;
      color: var(--text-secondary);
      font-weight: 400;
    }

    /* Sections */
    section {
      background: var(--bg-secondary);
      border-radius: var(--border-radius);
      padding: 2rem;
      margin-bottom: 1.5rem;
      box-shadow: var(--shadow);
      animation: fadeInUp 0.6s ease backwards;
      border: 1px solid rgba(255,255,255,0.08);
    }

    section:nth-of-type(1) { animation-delay: 0.1s; }
    section:nth-of-type(2) { animation-delay: 0.2s; }

    h2 {
      font-size: 1.5rem;
      font-weight: 600;
      margin-bottom: 1.25rem;
      display: flex;
      align-items: center;
      gap: 0.5rem;
      padding-bottom: 0.75rem;
      border-bottom: 2px solid rgba(99, 102, 241, 0.3);
    }

    h2 .badge {
      font-size: 0.75rem;
      padding: 0.25rem 0.6rem;
      border-radius: 20px;
      font-weight: 500;
    }

    h2 .badge.current {
      background: rgba(34, 197, 94, 0.15);
      color: var(--success);
    }

    h2 .badge.future {
      background: rgba(245, 158, 11, 0.15);
      color: var(--warning);
    }

    /* Lists */
    ul {
      list-style: none;
      display: grid;
      gap: 0.85rem;
    }

    li {
      display: flex;
      align-items: flex-start;
      gap: 0.75rem;
      padding: 0.85rem 1rem;
      background: var(--bg-card);
      border-radius: 10px;
      transition: var(--transition);
      border-left: 3px solid transparent;
    }

    li:hover {
      transform: translateX(4px);
      border-left-color: var(--accent);
      background: #475569;
    }

    li::before {
      content: "✓";
      flex-shrink: 0;
      width: 24px;
      height: 24px;
      background: linear-gradient(135deg, var(--accent), var(--accent-hover));
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 0.8rem;
      font-weight: 600;
      margin-top: 2px;
    }

    section.future li::before {
      content: "◦";
      background: linear-gradient(135deg, var(--warning), #fbbf24);
    }

    li strong {
      color: var(--text-primary);
      font-weight: 500;
    }

    li code {
      background: rgba(99, 102, 241, 0.2);
      padding: 0.15rem 0.45rem;
      border-radius: 6px;
      font-family: 'Fira Code', monospace;
      font-size: 0.9em;
      color: #c7d2fe;
    }

    /* Footer */
    footer {
      text-align: center;
      padding: 2rem 1rem;
      color: var(--text-secondary);
      font-size: 0.9rem;
      animation: fadeIn 1s ease 0.4s backwards;
    }

    footer .pulse {
      display: inline-block;
      width: 8px;
      height: 8px;
      background: var(--success);
      border-radius: 50%;
      margin-right: 0.5rem;
      animation: pulse 2s infinite;
    }

    /* Animations */
    @keyframes fadeInDown {
      from { opacity: 0; transform: translateY(-20px); }
      to { opacity: 1; transform: translateY(0); }
    }

    @keyframes fadeInUp {
      from { opacity: 0; transform: translateY(20px); }
      to { opacity: 1; transform: translateY(0); }
    }

    @keyframes fadeIn {
      from { opacity: 0; }
      to { opacity: 1; }
    }

    @keyframes pulse {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.6; transform: scale(0.9); }
    }

    /* Responsive */
    @media (max-width: 640px) {
      h1 { font-size: 2rem; }
      .subtitle { font-size: 1.1rem; }
      section { padding: 1.5rem; }
      h2 { font-size: 1.3rem; }
      li { padding: 0.75rem; }
    }
  </style>
</head>
<body>
  <div class="container">
    <!-- Header -->
    <header>
      <div class="logo">
        <div class="logo-icon">⚡</div>
        <h1>Поток</h1>
      </div>
      <p class="subtitle">Система управления задачами</p>
    </header>

    <!-- Current Features -->
    <section class="current">
      <h2>
        Поддерживает
        <span class="badge current">✓ готово</span>
      </h2>
      <ul>
        <li>Создание разных <strong>алгоритмов мониторинга</strong></li>
        <li>Приоритезацию задач <small>(каждый приоритет отправляется в свою очередь)</small></li>
        <li>Два типа задач: <code>«Без типа»</code> и <code>«Временной интервал»</code></li>
        <li>Отслеживание прогресса выполнения для задач типа <code>«Временной интервал»</code></li>
        <li>Историзм статусов задач</li>
      </ul>
    </section>

    <!-- Future Features -->
    <section class="future">
      <h2>
        В дальнейшем будет
        <span class="badge future">◦ в планах</span>
      </h2>
      <ul>
        <li>Реализована <strong>балансировка нагрузки</strong></li>
        <li>Добавлен тип задачи <code>«Пагинация»</code> с возможностью отслеживания прогресса</li>
        <li>Создан <strong>пользовательский интерфейс</strong></li>
        <li>Реализована и настроена <strong>аутентификация и авторизация</strong></li>
      </ul>
    </section>

    <!-- Footer -->
    <footer>
      <span class="pulse"></span>
      Система активно развивается • 2026
    </footer>
  </div>
</body>
</html>
    """