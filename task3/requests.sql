-- запрос №1
WITH monthly_totals AS (
    SELECT 
        DATE_TRUNC('MONTH', order_ts) AS month,
        course_id,
        SUM(order_amount) AS amount
    FROM orders
    GROUP BY 1, 2
),
rn_courses AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY amount DESC) AS rn
    FROM monthly_totals
)
SELECT 
    TO_CHAR(month, 'YYYY-MM') AS month,
    c.course_name
FROM rn_courses
	LEFT JOIN courses c USING (course_id)
WHERE rn <= 5
ORDER BY month, rn;

-- индекс для оптимизации запроса №1
CREATE INDEX idx_orders_month_course_id ON orders(DATE_TRUNC('MONTH', order_ts), course_id)

-- запрос №2
WITH top_subjects AS (
	SELECT subject_id,
		   package_id,
		   COUNT(package_id) AS package
	FROM orders
	GROUP BY 1, 2
),
subjects_rn AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY package DESC) AS rn
    FROM top_subjects
)
SELECT subjects.subject_name, packages.package_name
FROM subjects_rn
	LEFT JOIN subjects USING (subject_id)
	LEFT JOIN packages USING (package_id)
WHERE rn <= 3
ORDER BY 1, rn;

-- индекс для оптимизации запроса №1
CREATE INDEX idx_orders_subject_id_package_id ON orders(subject_id, package_id);