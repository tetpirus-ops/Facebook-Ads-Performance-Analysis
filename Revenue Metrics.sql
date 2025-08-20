-- ПІДГОТОВКА ДАНИХ, ПРИБИРАННЯ ЗАЙВОГО --

-- Перевіряю таблиці на актуальність та вміст --
SELECT * FROM project.deneme;  
SELECT COUNT(*) FROM project.deneme;  
-- тестова таблиця, 1 рядок за 2025 рік
-- можна ігнорувати через великий відрив у часі

SELECT * FROM project.deneme_2;  
SELECT COUNT(*) FROM project.deneme_2;  
-- 3026 рядків за 2022 рік
-- потенційно основна таблиця транзакцій

SELECT * FROM project.deneme_3;  
SELECT COUNT(*) FROM project.deneme_3;  
-- порожня таблиця
-- можна ігнорувати

SELECT * FROM project.deneme_4;  
SELECT COUNT(*) FROM project.deneme_4;  
-- 4 рядки
-- демографічні дані

SELECT * FROM project.games_paid_users;  
SELECT COUNT(*) FROM project.games_paid_users;  
-- 383 рядки
-- унікальні дані про пристрої

SELECT * FROM project.games_payments;  
SELECT COUNT(*) FROM project.games_payments;  
-- 3026 рядків
-- можливо, дублює deneme_2


-- Перевіряю, чи таблиці deneme_2 та games_payments однакові --
SELECT * FROM project.deneme_2
EXCEPT
SELECT * FROM project.games_payments;

SELECT * FROM project.games_payments
EXCEPT
SELECT * FROM project.deneme_2;

-- Відмінності не знайдені --

-- Перевіряю структур колонок, для остаточного висновка --
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name IN ('deneme_2', 'games_payments');

-- Висновок: таблиці однакові → використовую лише games_payments --


-- Перевіряю таблицю на наявність дублікатів всередині таблиці транзакцій --
SELECT user_id, game_name, payment_date, revenue_amount_usd,
  COUNT(*) AS duplicate_count
FROM project.games_payments
GROUP BY user_id, game_name, payment_date, revenue_amount_usd
HAVING COUNT(*) > 1;

-- Знайдено один дублікат --

-- Перевіряю природу дубліката --
SELECT *
FROM project.games_payments
WHERE user_id = 'F03P4YcFbbtcWUlujjIQmQ==' 
  AND game_name = 'game 3' 
  AND payment_date = '2022-10-15' 
  AND revenue_amount_usd = '21.78';

  -- Додаткових полів, які можуть засвідчити, що це подвійна оплата- немає --
  -- Роблю висновок, що це випадкове задвоєння даних --

-- Створюю очищену таблицю без дублікатів --
CREATE TABLE games_payments_clean AS
SELECT *
FROM (SELECT *,
    ROW_NUMBER() OVER (PARTITION BY user_id, game_name, payment_date, revenue_amount_usd ORDER BY user_id) AS row_num
  FROM project.games_payments) dublicates
WHERE row_num = 1;

-- Перевіряю кількості рядків після очищення --
SELECT COUNT(*) FROM project.games_payments;  -- 3026
SELECT COUNT(*) FROM games_payments_clean;    -- 3025

-- Перевіряю, чи дублюються демографічні дані у games_paid_users і deneme_4 --
-- 1.language
SELECT *
FROM (SELECT 
	gpc.user_id, 
	pgpu.language AS user_language, 
	pd4.language AS deneme_language
  FROM games_payments_clean gpc
  LEFT JOIN project.games_paid_users pgpu ON gpc.user_id = pgpu.user_id AND gpc.game_name = pgpu.game_name
  LEFT JOIN project.deneme_4 pd4 ON gpc.user_id = pd4.user_id) AS column_duplicate
WHERE user_language IS DISTINCT FROM deneme_language;

-- Висновок: deneme_4 містить часткові значення, дублюючі games_paid_users → language з цієї таблиці не використовуємо

-- 2.age
SELECT *
FROM (SELECT 
	gpc.user_id, 
	pgpu.age AS user_age, 
	pd4.age AS deneme_age
  FROM games_payments_clean gpc
  LEFT JOIN project.games_paid_users pgpu ON gpc.user_id = pgpu.user_id AND gpc.game_name = pgpu.game_name
  LEFT JOIN project.deneme_4 pd4 ON gpc.user_id = pd4.user_id) AS column_duplicate
WHERE user_age IS DISTINCT FROM deneme_age;

-- Виявлена розбіжність --

-- Досліджую скільки таких користувачів --
SELECT DISTINCT user_id
FROM (SELECT 
	gpc.user_id, pgpu.age AS user_age, 
	pd4.age AS deneme_age
  FROM games_payments_clean gpc
  LEFT JOIN project.games_paid_users pgpu ON gpc.user_id = pgpu.user_id AND gpc.game_name = pgpu.game_name
  LEFT JOIN project.deneme_4 pd4 ON gpc.user_id = pd4.user_id
  WHERE pd4.age IS NOT NULL) test
WHERE user_age IS DISTINCT FROM deneme_age;

-- Виявлена розбіжність лише в одного користувача --

-- Перевіряю всі транзакції цього користувача --
SELECT COUNT(*)
FROM games_payments_clean
WHERE user_id = 'VGEh7S+cxW9PT7H8KNgEGQ==';

-- Кількість збігається з першою ітерацією
-- Оскільки розбіжність не велика і лише в одного користувача з 383 можливих
-- Приймаю рішення: довіряти таблиці games_paid_users (точніша, обʼємніша)

-- Перевіряю характер total_spend з deneme_4 --
SELECT 
	gpc.user_id, 
	gpc.game_name, 
	gpc.payment_date, 
	gpc.revenue_amount_usd,
	pgpu.language AS user_language,
	pgpu.age AS user_age,
	pgpu.has_older_device_model,
	pd4.total_spend
FROM games_payments_clean as gpc
LEFT JOIN project.games_paid_users as pgpu ON gpc.user_id = pgpu.user_id AND gpc.game_name = pgpu.game_name
LEFT JOIN project.deneme_4 as pd4 ON gpc.user_id = pd4.user_id;

-- Багато null → таблицю deneme_4 не використовуємо далі



-- Отже, фінальні таблиці для подальшої роботи:
-- games_payments_clean – очищена таблиця транзакцій (3025 рядків)
-- games_paid_users – унікальні демографічні дані (383 рядки)




-- ПОЧИНАЮ БЕЗПОСЕРЕДНІЙ АНАЛІЗ ПОВЕДІНКИ КОРИСТУВАЧА --
 

-- Створюю агреговану таблицю для оцінки LT та LTV кожного користувача --
CREATE TABLE user_lifetime_metrics AS
SELECT
  user_id,
  MIN(payment_date) AS first_payment_date,  -- дата першої транзакції
  MAX(payment_date) AS last_payment_date,   -- дата останньої транзакції
  MAX(payment_date) - MIN(payment_date) AS LT_days,  -- тривалість життєвого циклу в днях
  DATE_PART('year', MAX(payment_date)) * 12 + DATE_PART('month', MAX(payment_date)) -
  DATE_PART('year', MIN(payment_date)) * 12 - DATE_PART('month', MIN(payment_date)) AS LT_months,  -- тривалість у місяцях
  SUM(revenue_amount_usd) AS LTV  -- сума всіх оплат користувача
FROM games_payments_clean
GROUP BY user_id;

SELECT * FROM user_lifetime_metrics;  --перевіряю таблицю


-- Створюю таблицю для помісячного аналізу користувачів: доходи, активність, статус --
CREATE TABLE user_monthly_metrics AS
WITH payments_by_user_month AS (SELECT
    user_id,
    DATE_TRUNC('month', payment_date) AS payment_month,
    SUM(revenue_amount_usd) AS monthly_revenue
  FROM games_payments_clean
  GROUP BY user_id, DATE_TRUNC('month', payment_date)),
user_first_last_payment AS (SELECT
    user_id,
    MIN(DATE_TRUNC('month', payment_date)) AS first_payment_month,
    MAX(DATE_TRUNC('month', payment_date)) AS last_payment_month
  FROM games_payments_clean
  GROUP BY user_id),
user_monthly_with_lag AS (SELECT
    pbum.user_id,
    pbum.payment_month,
    pbum.monthly_revenue,
    uflp.first_payment_month,
    uflp.last_payment_month,
    LAG(pbum.monthly_revenue) OVER (PARTITION BY pbum.user_id 
    ORDER BY pbum.payment_month) AS prev_month_revenue  -- значення доходу попереднього місяця (lag)
  FROM payments_by_user_month as pbum
  LEFT JOIN user_first_last_payment as uflp ON pbum.user_id = uflp.user_id),
final AS (SELECT *,
    CASE WHEN payment_month = first_payment_month THEN TRUE ELSE FALSE END AS is_new_user,
    CASE WHEN payment_month = last_payment_month THEN TRUE ELSE FALSE END AS is_churned_user,
    monthly_revenue - COALESCE(prev_month_revenue, 0) AS monthly_revenue_diff,
    CASE 
      WHEN payment_month = first_payment_month THEN 'new'
      WHEN payment_month = last_payment_month THEN 'churned'
      WHEN prev_month_revenue IS NULL THEN 'resurrected'
      ELSE 'retained'
    END AS status  -- класифікація користувача
  FROM user_monthly_with_lag)
SELECT * FROM final;

SELECT * FROM user_monthly_metrics;  --перевіряю таблицю


-- Підрахунок ключових метрик за місяць: Paid User, ARPPU, New MMR, MMR --
SELECT
  payment_month,
  COUNT(DISTINCT user_id) AS Paid_Users,
  SUM(monthly_revenue) AS Total_Revenue,
  ROUND(SUM(monthly_revenue)::numeric / COUNT(DISTINCT user_id), 2) AS ARPPU,
  SUM(CASE WHEN status = 'new' THEN monthly_revenue ELSE 0 END) AS New_MRR,
  SUM(CASE WHEN status = 'retained' THEN monthly_revenue ELSE 0 END) AS MRR
FROM user_monthly_metrics
GROUP BY payment_month
ORDER BY payment_month;


-- Визначення користувачів, які відпали (churned) --
WITH users_by_month AS (SELECT 
user_id,
payment_month,
monthly_revenue
  FROM user_monthly_metrics),
Churned_Users AS (SELECT 
    a.payment_month AS month,
    COUNT(DISTINCT a.user_id) AS Churned_Users,
    SUM(a.monthly_revenue) AS Churned_Revenue
  FROM users_by_month as a
  LEFT JOIN users_by_month as b
    ON a.user_id = b.user_id 
    AND b.payment_month = a.payment_month + INTERVAL '1 month'
  WHERE b.user_id IS NULL  -- немає активності в наступному місяці (churned)
  GROUP BY a.payment_month)
SELECT * FROM Churned_Users
ORDER BY month;


-- Розрахунок Churn Rate та Revenue Churn Rate з урахуванням попереднього місяця --
WITH Churned_Users AS (SELECT 
    a.payment_month AS month,
    COUNT(DISTINCT a.user_id) AS Churned_Users,
    SUM(a.monthly_revenue) AS Churned_Revenue
  FROM user_monthly_metrics as a
  LEFT JOIN user_monthly_metrics as b
    ON a.user_id = b.user_id 
    AND b.payment_month = a.payment_month + INTERVAL '1 month'
  WHERE b.user_id IS NULL
  GROUP BY a.payment_month),
monthly_metrics AS (SELECT
    payment_month,
    COUNT(DISTINCT user_id) AS Paid_Users,
    SUM(CASE WHEN status IN ('retained', 'resurrected') THEN monthly_revenue ELSE 0 END) AS MRR
  FROM user_monthly_metrics
  GROUP BY payment_month),
churn_with_prev_metrics AS (SELECT
    c.month,
    c.Churned_Users,
    c.Churned_Revenue,
    m.Paid_Users,
    m.MRR,
    LAG(m.Paid_Users) OVER (ORDER BY c.month) AS paid_users_prev_month,
    LAG(m.MRR) OVER (ORDER BY c.month) AS mrr_prev_month
  FROM Churned_Users as c
  LEFT JOIN monthly_metrics as m ON c.month = m.payment_month)
SELECT
  month,
  Churned_Users,
  Churned_Revenue,
  paid_users_prev_month,
  mrr_prev_month,
  ROUND(1.0 * Churned_Users / NULLIF(paid_users_prev_month, 0), 4) AS Churn_Rate,
  ROUND(1.0 * Churned_Revenue / NULLIF(mrr_prev_month, 0), 4) AS Revenue_Churn_Rate
FROM churn_with_prev_metrics
ORDER BY month;


-- Визначаю Expansion і Contraction MRR --
SELECT
  payment_month,
  SUM(CASE 
        WHEN prev_month_revenue IS NOT NULL AND monthly_revenue_diff > 0 THEN monthly_revenue_diff
        ELSE 0 END) AS Expansion_MRR,
  SUM(CASE 
        WHEN prev_month_revenue IS NOT NULL AND monthly_revenue_diff < 0 AND monthly_revenue > 0 THEN -monthly_revenue_diff
        ELSE 0 END) AS Contraction_MRR
FROM user_monthly_metrics
GROUP BY payment_month
ORDER BY payment_month;


-- Окремо створюю таблиці з додаванням демографічних даних --

CREATE TABLE user_monthly_metrics_with_demography AS
SELECT 
  umm.*,
  gpu.age,
  gpu.language,
  gpu.has_older_device_model
FROM user_monthly_metrics as umm
LEFT JOIN project.games_paid_users as gpu
  ON umm.user_id = gpu.user_id;

SELECT * FROM user_monthly_metrics_with_demography;  --перевіряю таблицю

CREATE TABLE user_lifetime_metrics_with_demography AS
SELECT 
  ulm.*,
  gpu.age,
  gpu.language,
  gpu.has_older_device_model
FROM user_lifetime_metrics as ulm
LEFT JOIN project.games_paid_users as gpu
  ON ulm.user_id = gpu.user_id;

SELECT * FROM user_lifetime_metrics_with_demography;  --перевіряю таблицю





-- ФІНАЛЬНА ТАБЛИЦЯ З УСІМА ПРОДУКТОВИМИ МЕТРИКАМИ 

CREATE TABLE monthly_product_metrics_ AS
WITH monthly_revenue AS (
  SELECT DATE_TRUNC('month',gp.payment_date)::date AS payment_month,gp.user_id,COALESCE(SUM(gp.revenue_amount_usd),0)::numeric AS total_revenue
  FROM games_payments_clean gp
  GROUP BY 1,2
),
lag_lead AS (
  SELECT mr.*,
         (mr.payment_month - INTERVAL '1 month')::date AS previous_calendar_month,
         (mr.payment_month + INTERVAL '1 month')::date AS next_calendar_month,
         LAG(mr.payment_month)  OVER (PARTITION BY mr.user_id ORDER BY mr.payment_month) AS previous_paid_month,
         LAG(mr.total_revenue)  OVER (PARTITION BY mr.user_id ORDER BY mr.payment_month) AS previous_paid_month_revenue,
         LEAD(mr.payment_month) OVER (PARTITION BY mr.user_id ORDER BY mr.payment_month) AS next_paid_month
  FROM monthly_revenue mr
),
revenue_metrics AS (
  SELECT payment_month,user_id,total_revenue,
         CASE WHEN previous_paid_month IS NULL THEN total_revenue END AS new_mrr,
         CASE WHEN previous_paid_month=previous_calendar_month AND previous_paid_month_revenue IS NOT NULL AND total_revenue>previous_paid_month_revenue THEN total_revenue-previous_paid_month_revenue END AS expansion_mrr,
         CASE WHEN previous_paid_month=previous_calendar_month AND previous_paid_month_revenue IS NOT NULL AND total_revenue<previous_paid_month_revenue THEN previous_paid_month_revenue-total_revenue END AS contraction_mrr_abs,
         CASE WHEN previous_paid_month IS NOT NULL AND previous_paid_month<>previous_calendar_month THEN total_revenue END AS back_from_churn_mrr,
         CASE WHEN next_paid_month IS NULL OR next_paid_month<>next_calendar_month THEN total_revenue END AS churn_mrr_event,
         CASE WHEN next_paid_month IS NULL OR next_paid_month<>next_calendar_month THEN 1 END AS churned_user_event,
         CASE WHEN next_paid_month IS NULL OR next_paid_month<>next_calendar_month THEN next_calendar_month END AS churn_month
  FROM lag_lead
),
metrics_with_dims AS (
  SELECT rm.*,gpu.language,gpu.age,gpu.has_older_device_model
  FROM revenue_metrics rm
  LEFT JOIN project.games_paid_users gpu ON rm.user_id=gpu.user_id
),
agg_by_payment AS (
  SELECT payment_month,language,age,has_older_device_model,
         COUNT(DISTINCT user_id) AS paid_users,
         SUM(total_revenue) AS mrr,
         SUM(new_mrr) AS new_mrr,
         COUNT(DISTINCT CASE WHEN new_mrr IS NOT NULL THEN user_id END) AS new_paid_users,
         SUM(expansion_mrr) AS expansion_mrr,
         SUM(COALESCE(contraction_mrr_abs,0))*(-1) AS contraction_mrr,
         SUM(back_from_churn_mrr) AS back_from_churn_mrr
  FROM metrics_with_dims
  GROUP BY 1,2,3,4
),
agg_by_churn AS (
  SELECT churn_month AS payment_month,language,age,has_older_device_model,
         SUM(churn_mrr_event) AS churn_mrr,
         SUM(churn_mrr_event) AS churned_revenue,
         SUM(churned_user_event) AS churned_users
  FROM metrics_with_dims
  WHERE churn_month IS NOT NULL
  GROUP BY 1,2,3,4
),
month_dim_keys AS (
  SELECT DISTINCT payment_month,language,age,has_older_device_model FROM agg_by_payment
  UNION
  SELECT DISTINCT payment_month,language,age,has_older_device_model FROM agg_by_churn
),
merged AS (
  SELECT k.payment_month,k.language,k.age,k.has_older_device_model,
         COALESCE(p.paid_users,0) AS paid_users,
         COALESCE(p.mrr,0) AS mrr,
         COALESCE(p.new_mrr,0) AS new_mrr,
         COALESCE(p.new_paid_users,0) AS new_paid_users,
         COALESCE(p.expansion_mrr,0) AS expansion_mrr,
         COALESCE(p.contraction_mrr,0) AS contraction_mrr,
         COALESCE(p.back_from_churn_mrr,0) AS back_from_churn_mrr,
         COALESCE(c.churned_users,0) AS churned_users,
         COALESCE(c.churn_mrr,0) AS churn_mrr,
         COALESCE(c.churned_revenue,0) AS churned_revenue
  FROM month_dim_keys k
  LEFT JOIN agg_by_payment p
    ON p.payment_month=k.payment_month
   AND p.language IS NOT DISTINCT FROM k.language
   AND p.age IS NOT DISTINCT FROM k.age
   AND p.has_older_device_model IS NOT DISTINCT FROM k.has_older_device_model
  LEFT JOIN agg_by_churn c
    ON c.payment_month=k.payment_month
   AND c.language IS NOT DISTINCT FROM k.language
   AND c.age IS NOT DISTINCT FROM k.age
   AND c.has_older_device_model IS NOT DISTINCT FROM k.has_older_device_model
)
SELECT payment_month,language,age,has_older_device_model,
       mrr AS "MRR",
       new_mrr AS "New MRR",
       expansion_mrr AS "Expansion MRR",
       contraction_mrr AS "Contraction MRR",
       churn_mrr AS "Churn MRR",
       churned_revenue AS "Churned Revenue",
       back_from_churn_mrr AS "Back from Churn MRR",
       paid_users AS "Paid Users",
       new_paid_users AS "New Paid Users",
       churned_users AS "Churned Users",
       LAG(paid_users) OVER (PARTITION BY language,age,has_older_device_model ORDER BY payment_month) AS "Paid Users previous month",
       LAG(mrr) OVER (PARTITION BY language,age,has_older_device_model ORDER BY payment_month) AS "MRR previous month",
       (payment_month - INTERVAL '1 month')::date AS "Previous payment month",
       (payment_month + INTERVAL '1 month')::date AS "Next payment month"
FROM merged
ORDER BY payment_month,language,age,has_older_device_model;

SELECT * FROM monthly_product_metrics_;  --перевіряю таблицю
