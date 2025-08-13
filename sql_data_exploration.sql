-- =================================================================
-- AMAZON PRODUCTS DATA EXPLORATION & QUALITY ASSESSMENT
-- =================================================================

-- Dataset Overview and Schema Analysis
-- =================================================================

-- 1. Basic dataset structure analysis
SELECT 'Total Records' as metric, COUNT(*) as value
FROM amazon_products
UNION ALL
SELECT 'Total Categories', COUNT(DISTINCT main_category)
FROM amazon_products
UNION ALL
SELECT 'Total Subcategories', COUNT(DISTINCT sub_category)
FROM amazon_products
UNION ALL
SELECT 'Products with Ratings', COUNT(*)
FROM amazon_products
WHERE ratings IS NOT NULL AND ratings != ''
UNION ALL
SELECT 'Products with Prices', COUNT(*)
FROM amazon_products
WHERE discount_price IS NOT NULL AND discount_price != '';

-- 2. Data type validation and conversion check
SELECT 
    name,
    main_category,
    sub_category,
    ratings,
    no_of_ratings,
    discount_price,
    actual_price,
    -- Attempt to identify numeric conversion issues
    CASE 
        WHEN ratings REGEXP '^[0-9]+\\.?[0-9]*$' THEN 'Valid Numeric'
        WHEN ratings IS NULL OR ratings = '' THEN 'Missing'
        ELSE 'Invalid Format'
    END as ratings_status,
    CASE 
        WHEN discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$' THEN 'Valid Price'
        WHEN discount_price IS NULL OR discount_price = '' THEN 'Missing'
        ELSE 'Invalid Format'
    END as price_status
FROM amazon_products
LIMIT 20;

-- 3. Missing value analysis by column
SELECT 
    'name' as column_name,
    COUNT(*) as total_records,
    COUNT(name) as non_null_count,
    COUNT(*) - COUNT(name) as null_count,
    ROUND((COUNT(*) - COUNT(name)) * 100.0 / COUNT(*), 2) as null_percentage
FROM amazon_products
UNION ALL
SELECT 
    'main_category',
    COUNT(*),
    COUNT(main_category),
    COUNT(*) - COUNT(main_category),
    ROUND((COUNT(*) - COUNT(main_category)) * 100.0 / COUNT(*), 2)
FROM amazon_products
UNION ALL
SELECT 
    'ratings',
    COUNT(*),
    COUNT(CASE WHEN ratings IS NOT NULL AND ratings != '' THEN 1 END),
    COUNT(*) - COUNT(CASE WHEN ratings IS NOT NULL AND ratings != '' THEN 1 END),
    ROUND((COUNT(*) - COUNT(CASE WHEN ratings IS NOT NULL AND ratings != '' THEN 1 END)) * 100.0 / COUNT(*), 2)
FROM amazon_products
UNION ALL
SELECT 
    'discount_price',
    COUNT(*),
    COUNT(CASE WHEN discount_price IS NOT NULL AND discount_price != '' THEN 1 END),
    COUNT(*) - COUNT(CASE WHEN discount_price IS NOT NULL AND discount_price != '' THEN 1 END),
    ROUND((COUNT(*) - COUNT(CASE WHEN discount_price IS NOT NULL AND discount_price != '' THEN 1 END)) * 100.0 / COUNT(*), 2)
FROM amazon_products;

-- 4. Duplicate identification
WITH duplicate_check AS (
    SELECT 
        name,
        main_category,
        sub_category,
        discount_price,
        COUNT(*) as duplicate_count
    FROM amazon_products
    GROUP BY name, main_category, sub_category, discount_price
    HAVING COUNT(*) > 1
)
SELECT 
    'Total Duplicate Groups' as metric,
    COUNT(*) as value
FROM duplicate_check
UNION ALL
SELECT 
    'Total Duplicate Records',
    SUM(duplicate_count)
FROM duplicate_check;

-- 5. Category distribution analysis
SELECT 
    main_category,
    COUNT(*) as product_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM amazon_products), 2) as percentage,
    COUNT(DISTINCT sub_category) as subcategory_count
FROM amazon_products
WHERE main_category IS NOT NULL
GROUP BY main_category
ORDER BY product_count DESC;

-- 6. Price range analysis by category
WITH price_cleaned AS (
    SELECT 
        main_category,
        sub_category,
        CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) as clean_price,
        CAST(REPLACE(REPLACE(REPLACE(actual_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) as clean_actual_price
    FROM amazon_products
    WHERE discount_price IS NOT NULL 
    AND discount_price != ''
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
)
SELECT 
    main_category,
    COUNT(*) as product_count,
    MIN(clean_price) as min_price,
    MAX(clean_price) as max_price,
    ROUND(AVG(clean_price), 2) as avg_price,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY clean_price), 2) as median_price,
    ROUND(STDDEV(clean_price), 2) as price_stddev
FROM price_cleaned
GROUP BY main_category
HAVING COUNT(*) >= 10
ORDER BY avg_price DESC;

-- 7. Rating distribution analysis
WITH rating_cleaned AS (
    SELECT 
        main_category,
        CAST(ratings AS DECIMAL(3,2)) as clean_rating,
        CAST(REPLACE(no_of_ratings, ',', '') AS INT) as clean_rating_count
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings != ''
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings != ''
)
SELECT 
    main_category,
    COUNT(*) as rated_products,
    MIN(clean_rating) as min_rating,
    MAX(clean_rating) as max_rating,
    ROUND(AVG(clean_rating), 2) as avg_rating,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY clean_rating), 2) as median_rating,
    SUM(clean_rating_count) as total_reviews,
    ROUND(AVG(clean_rating_count), 0) as avg_reviews_per_product
FROM rating_cleaned
GROUP BY main_category
HAVING COUNT(*) >= 10
ORDER BY avg_rating DESC;

-- 8. Data quality score by product
WITH quality_metrics AS (
    SELECT 
        name,
        main_category,
        sub_category,
        -- Quality scoring
        CASE WHEN name IS NOT NULL AND LENGTH(TRIM(name)) > 0 THEN 1 ELSE 0 END as has_name,
        CASE WHEN main_category IS NOT NULL AND LENGTH(TRIM(main_category)) > 0 THEN 1 ELSE 0 END as has_category,
        CASE WHEN ratings IS NOT NULL AND ratings REGEXP '^[0-5]\\.?[0-9]*$' THEN 1 ELSE 0 END as has_valid_rating,
        CASE WHEN discount_price IS NOT NULL AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$' THEN 1 ELSE 0 END as has_valid_price,
        CASE WHEN no_of_ratings IS NOT NULL AND no_of_ratings REGEXP '^[0-9,]+$' THEN 1 ELSE 0 END as has_rating_count,
        CASE WHEN image IS NOT NULL AND LENGTH(TRIM(image)) > 0 THEN 1 ELSE 0 END as has_image,
        CASE WHEN link IS NOT NULL AND LENGTH(TRIM(link)) > 0 THEN 1 ELSE 0 END as has_link
    FROM amazon_products
)
SELECT 
    (has_name + has_category + has_valid_rating + has_valid_price + 
     has_rating_count + has_image + has_link) as quality_score,
    COUNT(*) as product_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM amazon_products), 2) as percentage
FROM quality_metrics
GROUP BY (has_name + has_category + has_valid_rating + has_valid_price + 
          has_rating_count + has_image + has_link)
ORDER BY quality_score DESC;

-- 9. Price vs Rating correlation analysis
WITH clean_data AS (
    SELECT 
        main_category,
        CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) as price,
        CAST(ratings AS DECIMAL(3,2)) as rating,
        CAST(REPLACE(no_of_ratings, ',', '') AS INT) as review_count
    FROM amazon_products
    WHERE discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
    AND ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
),
price_rating_buckets AS (
    SELECT 
        main_category,
        CASE 
            WHEN price < 500 THEN 'Under ₹500'
            WHEN price < 1000 THEN '₹500-₹1000'
            WHEN price < 2000 THEN '₹1000-₹2000'
            WHEN price < 5000 THEN '₹2000-₹5000'
            ELSE 'Above ₹5000'
        END as price_bucket,
        rating,
        review_count
    FROM clean_data
)
SELECT 
    main_category,
    price_bucket,
    COUNT(*) as product_count,
    ROUND(AVG(rating), 2) as avg_rating,
    ROUND(AVG(review_count), 0) as avg_reviews
FROM price_rating_buckets
GROUP BY main_category, price_bucket
HAVING COUNT(*) >= 5
ORDER BY main_category, 
         CASE price_bucket 
             WHEN 'Under ₹500' THEN 1
             WHEN '₹500-₹1000' THEN 2
             WHEN '₹1000-₹2000' THEN 3
             WHEN '₹2000-₹5000' THEN 4
             WHEN 'Above ₹5000' THEN 5
         END;

-- 10. Summary statistics for final data quality report
WITH summary_stats AS (
    SELECT 
        COUNT(*) as total_products,
        COUNT(DISTINCT main_category) as total_categories,
        COUNT(CASE WHEN ratings IS NOT NULL AND ratings REGEXP '^[0-5]\\.?[0-9]*$' THEN 1 END) as products_with_ratings,
        COUNT(CASE WHEN discount_price IS NOT NULL AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$' THEN 1 END) as products_with_prices,
        COUNT(CASE WHEN ratings IS NOT NULL AND ratings REGEXP '^[0-5]\\.?[0-9]*$' 
                   AND discount_price IS NOT NULL AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$' THEN 1 END) as complete_products
    FROM amazon_products
)
SELECT 
    'Data Quality Summary' as report_section,
    CONCAT('Total Products: ', total_products) as metric_1,
    CONCAT('Categories: ', total_categories) as metric_2,
    CONCAT('Rating Coverage: ', ROUND(products_with_ratings * 100.0 / total_products, 1), '%') as metric_3,
    CONCAT('Price Coverage: ', ROUND(products_with_prices * 100.0 / total_products, 1), '%') as metric_4,
    CONCAT('Complete Records: ', ROUND(complete_products * 100.0 / total_products, 1), '%') as metric_5
FROM summary_stats;