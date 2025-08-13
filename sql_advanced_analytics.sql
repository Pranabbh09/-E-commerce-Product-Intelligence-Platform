-- =================================================================
-- ADVANCED SQL ANALYTICS FOR AMAZON PRODUCTS
-- Complex Window Functions, CTEs, and Business Intelligence Queries
-- =================================================================

-- 1. Top Performing Products by Category with Rankings
-- =================================================================
WITH product_metrics AS (
    SELECT 
        name,
        main_category,
        sub_category,
        CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) as price,
        CAST(REPLACE(REPLACE(REPLACE(actual_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) as original_price,
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
ranked_products AS (
    SELECT 
        *,
        -- Calculate discount percentage
        ROUND((original_price - price) * 100.0 / original_price, 2) as discount_percentage,
        -- Ranking by different metrics
        ROW_NUMBER() OVER (PARTITION BY main_category ORDER BY rating DESC, review_count DESC) as rating_rank,
        ROW_NUMBER() OVER (PARTITION BY main_category ORDER BY review_count DESC) as popularity_rank,
        ROW_NUMBER() OVER (PARTITION BY main_category ORDER BY price ASC) as price_rank,
        -- Percentile rankings
        NTILE(4) OVER (PARTITION BY main_category ORDER BY rating) as rating_quartile,
        NTILE(4) OVER (PARTITION BY main_category ORDER BY review_count) as popularity_quartile,
        -- Calculate composite score
        (rating * 0.4 + (review_count/1000) * 0.3 + ((original_price - price)/original_price * 100) * 0.3) as composite_score
    FROM product_metrics
    WHERE original_price > price -- Only products with actual discounts
)
SELECT 
    main_category,
    name,
    rating,
    review_count,
    price,
    original_price,
    discount_percentage,
    ROUND(composite_score, 2) as composite_score,
    rating_rank,
    popularity_rank,
    CASE 
        WHEN rating_quartile = 4 THEN 'Top Rated'
        WHEN rating_quartile = 3 THEN 'Good'
        WHEN rating_quartile = 2 THEN 'Average'
        ELSE 'Below Average'
    END as rating_tier
FROM ranked_products
WHERE rating_rank <= 5  -- Top 5 products per category
ORDER BY main_category, rating_rank;

-- 2. Category Performance Dashboard Metrics
-- =================================================================
WITH category_analysis AS (
    SELECT 
        main_category,
        COUNT(*) as total_products,
        COUNT(CASE WHEN CAST(ratings AS DECIMAL(3,2)) >= 4.0 THEN 1 END) as high_rated_products,
        ROUND(AVG(CAST(ratings AS DECIMAL(3,2))), 2) as avg_rating,
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as avg_price,
        SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as total_reviews,
        MIN(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))) as min_price,
        MAX(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))) as max_price
    FROM amazon_products
    WHERE discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
    AND ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    GROUP BY main_category
),
category_rankings AS (
    SELECT 
        *,
        ROUND(high_rated_products * 100.0 / total_products, 2) as high_rating_percentage,
        RANK() OVER (ORDER BY avg_rating DESC) as rating_rank,
        RANK() OVER (ORDER BY total_reviews DESC) as review_volume_rank,
        RANK() OVER (ORDER BY total_products DESC) as product_count_rank,
        -- Market positioning
        CASE 
            WHEN avg_price > 2000 THEN 'Premium'
            WHEN avg_price > 500 THEN 'Mid-Range'
            ELSE 'Budget'
        END as price_segment
    FROM category_analysis
)
SELECT 
    main_category,
    total_products,
    avg_rating,
    high_rating_percentage,
    avg_price,
    price_segment,
    total_reviews,
    rating_rank,
    review_volume_rank,
    CASE 
        WHEN rating_rank <= 3 AND review_volume_rank <= 5 THEN 'Top Performer'
        WHEN rating_rank <= 5 OR review_volume_rank <= 5 THEN 'Strong Performer'
        WHEN rating_rank <= 10 AND review_volume_rank <= 10 THEN 'Average Performer'
        ELSE 'Underperformer'
    END as category_status
FROM category_rankings
ORDER BY rating_rank;

-- 3. Price Optimization Analysis with Window Functions
-- =================================================================
WITH price_analysis AS (
    SELECT 
        main_category,
        sub_category,
        name,
        CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) as current_price,
        CAST(ratings AS DECIMAL(3,2)) as rating,
        CAST(REPLACE(no_of_ratings, ',', '') AS INT) as review_count,
        -- Price percentiles within category
        PERCENTILE_CONT(0.25) OVER (PARTITION BY main_category) as price_p25,
        PERCENTILE_CONT(0.50) OVER (PARTITION BY main_category) as price_median,
        PERCENTILE_CONT(0.75) OVER (PARTITION BY main_category) as price_p75,
        -- Rating percentiles within category
        PERCENTILE_CONT(0.50) OVER (PARTITION BY main_category) as rating_median,
        PERCENTILE_CONT(0.75) OVER (PARTITION BY main_category) as rating_p75,
        -- Moving averages
        AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))) 
            OVER (PARTITION BY main_category ORDER BY CAST(ratings AS DECIMAL(3,2)) 
                  ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as price_moving_avg
    FROM amazon_products
    WHERE discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
    AND ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
),
price_recommendations AS (
    SELECT 
        *,
        CASE 
            WHEN current_price < price_p25 AND rating > rating_p75 THEN 'Underpriced - Consider Price Increase'
            WHEN current_price > price_p75 AND rating < rating_median THEN 'Overpriced - Consider Price Reduction'
            WHEN current_price BETWEEN price_p25 AND price_p75 AND rating > rating_p75 THEN 'Well Positioned - Premium Opportunity'
            WHEN current_price BETWEEN price_p25 AND price_p75 AND rating BETWEEN rating_median AND rating_p75 THEN 'Optimally Priced'
            ELSE 'Needs Analysis'
        END as pricing_recommendation,
        ABS(current_price - price_moving_avg) as price_deviation,
        CASE 
            WHEN current_price > price_moving_avg * 1.2 THEN 'High Premium'
            WHEN current_price > price_moving_avg * 1.1 THEN 'Premium'
            WHEN current_price < price_moving_avg * 0.9 THEN 'Discount'
            WHEN current_price < price_moving_avg * 0.8 THEN 'Deep Discount'
            ELSE 'Market Rate'
        END as pricing_position
    FROM price_analysis
)
SELECT 
    main_category,
    pricing_recommendation,
    COUNT(*) as product_count,
    ROUND(AVG(current_price), 2) as avg_current_price,
    ROUND(AVG(rating), 2) as avg_rating,
    ROUND(AVG(review_count), 0) as avg_reviews
FROM price_recommendations
GROUP BY main_category, pricing_recommendation
HAVING COUNT(*) >= 3
ORDER BY main_category, product_count DESC;

-- 4. Customer Satisfaction Analysis with Complex CTEs
-- =================================================================
WITH satisfaction_metrics AS (
    SELECT 
        main_category,
        sub_category,
        name,
        CAST(ratings AS DECIMAL(3,2)) as rating,
        CAST(REPLACE(no_of_ratings, ',', '') AS INT) as review_count,
        CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) as price,
        -- Satisfaction scoring
        CASE 
            WHEN CAST(ratings AS DECIMAL(3,2)) >= 4.5 THEN 'Highly Satisfied'
            WHEN CAST(ratings AS DECIMAL(3,2)) >= 4.0 THEN 'Satisfied'
            WHEN CAST(ratings AS DECIMAL(3,2)) >= 3.5 THEN 'Neutral'
            WHEN CAST(ratings AS DECIMAL(3,2)) >= 3.0 THEN 'Dissatisfied'
            ELSE 'Highly Dissatisfied'
        END as satisfaction_level,
        -- Review volume categories
        CASE 
            WHEN CAST(REPLACE(no_of_ratings, ',', '') AS INT) >= 1000 THEN 'High Volume'
            WHEN CAST(REPLACE(no_of_ratings, ',', '') AS INT) >= 100 THEN 'Medium Volume'
            WHEN CAST(REPLACE(no_of_ratings, ',', '') AS INT) >= 10 THEN 'Low Volume'
            ELSE 'Very Low Volume'
        END as review_volume_category
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
),
category_satisfaction AS (
    SELECT 
        main_category,
        satisfaction_level,
        review_volume_category,
        COUNT(*) as product_count,
        ROUND(AVG(rating), 2) as avg_rating,
        ROUND(AVG(price), 2) as avg_price,
        SUM(review_count) as total_reviews,
        -- Calculate satisfaction score weighted by review count
        ROUND(SUM(rating * review_count) / SUM(review_count), 2) as weighted_satisfaction_score
    FROM satisfaction_metrics
    GROUP BY main_category, satisfaction_level, review_volume_category
),
category_summary AS (
    SELECT 
        main_category,
        SUM(product_count) as total_products,
        SUM(CASE WHEN satisfaction_level IN ('Highly Satisfied', 'Satisfied') THEN product_count ELSE 0 END) as satisfied_products,
        ROUND(SUM(CASE WHEN satisfaction_level IN ('Highly Satisfied', 'Satisfied') THEN product_count ELSE 0 END) * 100.0 / SUM(product_count), 2) as satisfaction_rate,
        ROUND(AVG(weighted_satisfaction_score), 2) as overall_satisfaction_score
    FROM category_satisfaction
    GROUP BY main_category
)
SELECT 
    cs.main_category,
    cs.satisfaction_level,
    cs.review_volume_category,
    cs.product_count,
    cs.avg_rating,
    cs.avg_price,
    cs.total_reviews,
    cs.weighted_satisfaction_score,
    css.satisfaction_rate as category_satisfaction_rate,
    css.overall_satisfaction_score as category_overall_score
FROM category_satisfaction cs
JOIN category_summary css ON cs.main_category = css.main_category
WHERE cs.product_count >= 5  -- Focus on significant segments
ORDER BY cs.main_category, cs.satisfaction_level DESC, cs.review_volume_category DESC;

-- 5. Product Performance Cohort Analysis
-- =================================================================
WITH product_cohorts AS (
    SELECT 
        name,
        main_category,
        sub_category,
        CAST(ratings AS DECIMAL(3,2)) as rating,
        CAST(REPLACE(no_of_ratings, ',', '') AS INT) as review_count,
        CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) as price,
        -- Create cohorts based on performance metrics
        NTILE(4) OVER (ORDER BY CAST(ratings AS DECIMAL(3,2))) as rating_cohort,
        NTILE(4) OVER (ORDER BY CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as volume_cohort,
        NTILE(4) OVER (ORDER BY CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))) as price_cohort
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
),
cohort_analysis AS (
    SELECT 
        rating_cohort,
        volume_cohort,
        price_cohort,
        COUNT(*) as product_count,
        ROUND(AVG(rating), 2) as avg_rating,
        ROUND(AVG(review_count), 0) as avg_reviews,
        ROUND(AVG(price), 2) as avg_price,
        -- Performance classification
        CASE 
            WHEN rating_cohort = 4 AND volume_cohort = 4 THEN 'Stars'
            WHEN rating_cohort = 4 AND volume_cohort >= 3 THEN 'Rising Stars'
            WHEN rating_cohort >= 3 AND volume_cohort = 4 THEN 'Popular'
            WHEN rating_cohort >= 3 AND volume_cohort >= 3 THEN 'Solid Performers'
            WHEN rating_cohort <= 2 AND volume_cohort >= 3 THEN 'High Volume Low Rating'
            WHEN rating_cohort >= 3 AND volume_cohort <= 2 THEN 'Hidden Gems'
            ELSE 'Underperformers'
        END as performance_segment
    FROM product_cohorts
    GROUP BY rating_cohort, volume_cohort, price_cohort
)
SELECT 
    performance_segment,
    COUNT(*) as cohort_count,
    SUM(product_count) as total_products,
    ROUND(AVG(avg_rating), 2) as segment_avg_rating,
    ROUND(AVG(avg_reviews), 0) as segment_avg_reviews,
    ROUND(AVG(avg_price), 2) as segment_avg_price,
    ROUND(SUM(product_count) * 100.0 / (SELECT SUM(product_count) FROM cohort_analysis), 2) as market_share_percent
FROM cohort_analysis
GROUP BY performance_segment
ORDER BY total_products DESC;

-- 6. Advanced Business Intelligence Summary
-- =================================================================
WITH business_insights AS (
    SELECT 
        main_category,
        COUNT(*) as total_products,
        ROUND(AVG(CAST(ratings AS DECIMAL(3,2))), 2) as avg_rating,
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as avg_price,
        SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as total_customer_engagement,
        -- Business metrics
        COUNT(CASE WHEN CAST(ratings AS DECIMAL(3,2)) >= 4.0 THEN 1 END) as high_quality_products,
        COUNT(CASE WHEN CAST(REPLACE(no_of_ratings, ',', '') AS INT) >= 100 THEN 1 END) as established_products,
        -- Revenue potential (assuming review count correlates with sales)
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) * 
                  CAST(REPLACE(no_of_ratings, ',', '') AS INT)), 2) as revenue_potential_index
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
),
category_satisfaction AS (
    SELECT 
        main_category,
        satisfaction_level,
        review_volume_category,
        COUNT(*) as product_count,
        ROUND(AVG(rating), 2) as avg_rating,
        ROUND(AVG(price), 2) as avg_price,
        SUM(review_count) as total_reviews,
        -- Calculate satisfaction score weighted by review count
        ROUND(SUM(rating * review_count) / SUM(review_count), 2) as weighted_satisfaction_score
    FROM satisfaction_metrics
    GROUP BY main_category, satisfaction_level, review_volume_category
),
category_summary AS (
    SELECT 
        main_category,
        SUM(product_count) as total_products,
        SUM(CASE WHEN satisfaction_level IN ('Highly Satisfied', 'Satisfied') THEN product_count ELSE 0 END) as satisfied_products,
        ROUND(SUM(CASE WHEN satisfaction_level IN ('Highly Satisfied', 'Satisfied') THEN product_count ELSE 0 END) * 100.0 / SUM(product_count), 2) as satisfaction_rate,
        ROUND(AVG(weighted_satisfaction_score), 2) as overall_satisfaction_score
    FROM category_satisfaction
    GROUP BY main_category
)
SELECT 
    cs.main_category,
    cs.satisfaction_level,
    cs.review_volume_category,
    cs.product_count,
    cs.avg_rating,
    cs.avg_price,
    cs.total_reviews,
    cs.weighted_satisfaction_score,
    css.satisfaction_rate as category_satisfaction_rate,
    css.overall_satisfaction_score as category_overall_score
FROM category_satisfaction cs
JOIN category_summary css ON cs.main_category = css.main_category
WHERE cs.product_count >= 5  -- Focus on significant segments
ORDER BY cs.main_category, cs.satisfaction_level DESC, cs.review_volume_category DESC;

-- 5. Product Performance Cohort Analysis
-- =================================================================
WITH product_cohorts AS (
    SELECT 
        name,
        main_category,
        sub_category,
        CAST(ratings AS DECIMAL(3,2)) as rating,
        CAST(REPLACE(no_of_ratings, ',', '') AS INT) as review_count,
        CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) as price,
        -- Create cohorts based on performance metrics
        NTILE(4) OVER (ORDER BY CAST(ratings AS DECIMAL(3,2))) as rating_cohort,
        NTILE(4) OVER (ORDER BY CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as volume_cohort,
        NTILE(4) OVER (ORDER BY CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))) as price_cohort
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
),
cohort_analysis AS (
    SELECT 
        rating_cohort,
        volume_cohort,
        price_cohort,
        COUNT(*) as product_count,
        ROUND(AVG(rating), 2) as avg_rating,
        ROUND(AVG(review_count), 0) as avg_reviews,
        ROUND(AVG(price), 2) as avg_price,
        -- Performance classification
        CASE 
            WHEN rating_cohort = 4 AND volume_cohort = 4 THEN 'Stars'
            WHEN rating_cohort = 4 AND volume_cohort >= 3 THEN 'Rising Stars'
            WHEN rating_cohort >= 3 AND volume_cohort = 4 THEN 'Popular'
            WHEN rating_cohort >= 3 AND volume_cohort >= 3 THEN 'Solid Performers'
            WHEN rating_cohort <= 2 AND volume_cohort >= 3 THEN 'High Volume Low Rating'
            WHEN rating_cohort >= 3 AND volume_cohort <= 2 THEN 'Hidden Gems'
            ELSE 'Underperformers'
        END as performance_segment
    FROM product_cohorts
    GROUP BY rating_cohort, volume_cohort, price_cohort
)
SELECT 
    performance_segment,
    COUNT(*) as cohort_count,
    SUM(product_count) as total_products,
    ROUND(AVG(avg_rating), 2) as segment_avg_rating,
    ROUND(AVG(avg_reviews), 0) as segment_avg_reviews,
    ROUND(AVG(avg_price), 2) as segment_avg_price,
    ROUND(SUM(product_count) * 100.0 / (SELECT SUM(product_count) FROM cohort_analysis), 2) as market_share_percent
FROM cohort_analysis
GROUP BY performance_segment
ORDER BY total_products DESC;

-- 6. Advanced Business Intelligence Summary
-- =================================================================
WITH business_insights AS (
    SELECT 
        main_category,
        COUNT(*) as total_products,
        ROUND(AVG(CAST(ratings AS DECIMAL(3,2))), 2) as avg_rating,
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as avg_price,
        SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as total_customer_engagement,
        -- Business metrics
        COUNT(CASE WHEN CAST(ratings AS DECIMAL(3,2)) >= 4.0 THEN 1 END) as high_quality_products,
        COUNT(CASE WHEN CAST(REPLACE(no_of_ratings, ',', '') AS INT) >= 100 THEN 1 END) as established_products,
        -- Revenue potential (assuming review count correlates with sales)
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) * 
                  CAST(REPLACE(no_of_ratings, ',', '') AS INT)), 2) as revenue_potential_index
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
),
category_satisfaction AS (
    SELECT 
        main_category,
        satisfaction_level,
        review_volume_category,
        COUNT(*) as product_count,
        ROUND(AVG(rating), 2) as avg_rating,
        ROUND(AVG(price), 2) as avg_price,
        SUM(review_count) as total_reviews,
        -- Calculate satisfaction score weighted by review count
        ROUND(SUM(rating * review_count) / SUM(review_count), 2) as weighted_satisfaction_score
    FROM satisfaction_metrics
    GROUP BY main_category, satisfaction_level, review_volume_category
),
category_summary AS (
    SELECT 
        main_category,
        SUM(product_count) as total_products,
        SUM(CASE WHEN satisfaction_level IN ('Highly Satisfied', 'Satisfied') THEN product_count ELSE 0 END) as satisfied_products,
        ROUND(SUM(CASE WHEN satisfaction_level IN ('Highly Satisfied', 'Satisfied') THEN product_count ELSE 0 END) * 100.0 / SUM(product_count), 2) as satisfaction_rate,
        ROUND(AVG(weighted_satisfaction_score), 2) as overall_satisfaction_score
    FROM category_satisfaction
    GROUP BY main_category
)
SELECT 
    cs.main_category,
    cs.satisfaction_level,
    cs.review_volume_category,
    cs.product_count,
    cs.avg_rating,
    cs.avg_price,
    cs.total_reviews,
    cs.weighted_satisfaction_score,
    css.satisfaction_rate as category_satisfaction_rate,
    css.overall_satisfaction_score as category_overall_score
FROM category_satisfaction cs
JOIN category_summary css ON cs.main_category = css.main_category
WHERE cs.product_count >= 5  -- Focus on significant segments
ORDER BY cs.main_category, cs.satisfaction_level DESC, cs.review_volume_category DESC;

-- 5. Product Performance Cohort Analysis
-- =================================================================
WITH product_cohorts AS (
    SELECT 
        name,
        main_category,
        sub_category,
        CAST(ratings AS DECIMAL(3,2)) as rating,
        CAST(REPLACE(no_of_ratings, ',', '') AS INT) as review_count,
        CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) as price,
        -- Create cohorts based on performance metrics
        NTILE(4) OVER (ORDER BY CAST(ratings AS DECIMAL(3,2))) as rating_cohort,
        NTILE(4) OVER (ORDER BY CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as volume_cohort,
        NTILE(4) OVER (ORDER BY CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))) as price_cohort
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
),
cohort_analysis AS (
    SELECT 
        rating_cohort,
        volume_cohort,
        price_cohort,
        COUNT(*) as product_count,
        ROUND(AVG(rating), 2) as avg_rating,
        ROUND(AVG(review_count), 0) as avg_reviews,
        ROUND(AVG(price), 2) as avg_price,
        -- Performance classification
        CASE 
            WHEN rating_cohort = 4 AND volume_cohort = 4 THEN 'Stars'
            WHEN rating_cohort = 4 AND volume_cohort >= 3 THEN 'Rising Stars'
            WHEN rating_cohort >= 3 AND volume_cohort = 4 THEN 'Popular'
            WHEN rating_cohort >= 3 AND volume_cohort >= 3 THEN 'Solid Performers'
            WHEN rating_cohort <= 2 AND volume_cohort >= 3 THEN 'High Volume Low Rating'
            WHEN rating_cohort >= 3 AND volume_cohort <= 2 THEN 'Hidden Gems'
            ELSE 'Underperformers'
        END as performance_segment
    FROM product_cohorts
    GROUP BY rating_cohort, volume_cohort, price_cohort
)
SELECT 
    performance_segment,
    COUNT(*) as cohort_count,
    SUM(product_count) as total_products,
    ROUND(AVG(avg_rating), 2) as segment_avg_rating,
    ROUND(AVG(avg_reviews), 0) as segment_avg_reviews,
    ROUND(AVG(avg_price), 2) as segment_avg_price,
    ROUND(SUM(product_count) * 100.0 / (SELECT SUM(product_count) FROM cohort_analysis), 2) as market_share_percent
FROM cohort_analysis
GROUP BY performance_segment
ORDER BY total_products DESC;

-- 6. Advanced Business Intelligence Summary
-- =================================================================
WITH business_insights AS (
    SELECT 
        main_category,
        COUNT(*) as total_products,
        ROUND(AVG(CAST(ratings AS DECIMAL(3,2))), 2) as avg_rating,
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as avg_price,
        SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as total_customer_engagement,
        -- Business metrics
        COUNT(CASE WHEN CAST(ratings AS DECIMAL(3,2)) >= 4.0 THEN 1 END) as high_quality_products,
        COUNT(CASE WHEN CAST(REPLACE(no_of_ratings, ',', '') AS INT) >= 100 THEN 1 END) as established_products,
        -- Revenue potential (assuming review count correlates with sales)
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) * 
                  CAST(REPLACE(no_of_ratings, ',', '') AS INT)), 2) as revenue_potential_index
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    GROUP BY main_category
),
strategic_recommendations AS (
    SELECT 
        *,
        ROUND(high_quality_products * 100.0 / total_products, 2) as quality_rate,
        ROUND(established_products * 100.0 / total_products, 2) as establishment_rate,
        RANK() OVER (ORDER BY revenue_potential_index DESC) as revenue_rank,
        RANK() OVER (ORDER BY total_customer_engagement DESC) as engagement_rank,
        -- Strategic positioning
        CASE 
            WHEN avg_rating >= 4.2 AND revenue_potential_index >= 50000 THEN 'Market Leader'
            WHEN avg_rating >= 4.0 AND total_customer_engagement >= 10000 THEN 'Strong Contender'
            WHEN avg_rating >= 3.8 AND total_products >= 50 THEN 'Growing Category'
            WHEN avg_rating >= 3.5 THEN 'Improvement Needed'
            ELSE 'Strategic Review Required'
        END as strategic_position
    FROM business_insights
)
SELECT 
    main_category,
    total_products,
    avg_rating,
    quality_rate,
    avg_price,
    total_customer_engagement,
    revenue_potential_index,
    strategic_position,
    CASE 
        WHEN strategic_position = 'Market Leader' THEN 'Maintain excellence, expand product line'
        WHEN strategic_position = 'Strong Contender' THEN 'Focus on premium products, enhance marketing'
        WHEN strategic_position = 'Growing Category' THEN 'Improve quality, increase customer engagement'
        WHEN strategic_position = 'Improvement Needed' THEN 'Quality improvement program, customer feedback analysis'
        ELSE 'Comprehensive category review and repositioning'
    END as strategic_recommendation
FROM strategic_recommendations
ORDER BY revenue_rank;

-- 7. Market Opportunity Analysis
-- =================================================================
WITH market_gaps AS (
    SELECT 
        main_category,
        sub_category,
        COUNT(*) as current_products,
        ROUND(AVG(CAST(ratings AS DECIMAL(3,2))), 2) as avg_rating,
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as avg_price,
        SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as total_demand_signal,
        -- Identify gaps
        CASE 
            WHEN COUNT(*) < 10 AND AVG(CAST(ratings AS DECIMAL(3,2))) >= 4.0 THEN 'Underserved High Quality'
            WHEN COUNT(*) < 20 AND SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) >= 1000 THEN 'High Demand Low Supply'
            WHEN COUNT(*) >= 50 AND AVG(CAST(ratings AS DECIMAL(3,2))) < 3.5 THEN 'Quality Gap Opportunity'
            WHEN COUNT(*) >= 30 AND AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))) >= 2000 THEN 'Budget Option Gap'
            ELSE 'Saturated'
        END as market_opportunity
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
),
category_satisfaction AS (
    SELECT 
        main_category,
        satisfaction_level,
        review_volume_category,
        COUNT(*) as product_count,
        ROUND(AVG(rating), 2) as avg_rating,
        ROUND(AVG(price), 2) as avg_price,
        SUM(review_count) as total_reviews,
        -- Calculate satisfaction score weighted by review count
        ROUND(SUM(rating * review_count) / SUM(review_count), 2) as weighted_satisfaction_score
    FROM satisfaction_metrics
    GROUP BY main_category, satisfaction_level, review_volume_category
),
category_summary AS (
    SELECT 
        main_category,
        SUM(product_count) as total_products,
        SUM(CASE WHEN satisfaction_level IN ('Highly Satisfied', 'Satisfied') THEN product_count ELSE 0 END) as satisfied_products,
        ROUND(SUM(CASE WHEN satisfaction_level IN ('Highly Satisfied', 'Satisfied') THEN product_count ELSE 0 END) * 100.0 / SUM(product_count), 2) as satisfaction_rate,
        ROUND(AVG(weighted_satisfaction_score), 2) as overall_satisfaction_score
    FROM category_satisfaction
    GROUP BY main_category
)
SELECT 
    cs.main_category,
    cs.satisfaction_level,
    cs.review_volume_category,
    cs.product_count,
    cs.avg_rating,
    cs.avg_price,
    cs.total_reviews,
    cs.weighted_satisfaction_score,
    css.satisfaction_rate as category_satisfaction_rate,
    css.overall_satisfaction_score as category_overall_score
FROM category_satisfaction cs
JOIN category_summary css ON cs.main_category = css.main_category
WHERE cs.product_count >= 5  -- Focus on significant segments
ORDER BY cs.main_category, cs.satisfaction_level DESC, cs.review_volume_category DESC;

-- 5. Product Performance Cohort Analysis
-- =================================================================
WITH product_cohorts AS (
    SELECT 
        name,
        main_category,
        sub_category,
        CAST(ratings AS DECIMAL(3,2)) as rating,
        CAST(REPLACE(no_of_ratings, ',', '') AS INT) as review_count,
        CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) as price,
        -- Create cohorts based on performance metrics
        NTILE(4) OVER (ORDER BY CAST(ratings AS DECIMAL(3,2))) as rating_cohort,
        NTILE(4) OVER (ORDER BY CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as volume_cohort,
        NTILE(4) OVER (ORDER BY CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))) as price_cohort
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
),
cohort_analysis AS (
    SELECT 
        rating_cohort,
        volume_cohort,
        price_cohort,
        COUNT(*) as product_count,
        ROUND(AVG(rating), 2) as avg_rating,
        ROUND(AVG(review_count), 0) as avg_reviews,
        ROUND(AVG(price), 2) as avg_price,
        -- Performance classification
        CASE 
            WHEN rating_cohort = 4 AND volume_cohort = 4 THEN 'Stars'
            WHEN rating_cohort = 4 AND volume_cohort >= 3 THEN 'Rising Stars'
            WHEN rating_cohort >= 3 AND volume_cohort = 4 THEN 'Popular'
            WHEN rating_cohort >= 3 AND volume_cohort >= 3 THEN 'Solid Performers'
            WHEN rating_cohort <= 2 AND volume_cohort >= 3 THEN 'High Volume Low Rating'
            WHEN rating_cohort >= 3 AND volume_cohort <= 2 THEN 'Hidden Gems'
            ELSE 'Underperformers'
        END as performance_segment
    FROM product_cohorts
    GROUP BY rating_cohort, volume_cohort, price_cohort
)
SELECT 
    performance_segment,
    COUNT(*) as cohort_count,
    SUM(product_count) as total_products,
    ROUND(AVG(avg_rating), 2) as segment_avg_rating,
    ROUND(AVG(avg_reviews), 0) as segment_avg_reviews,
    ROUND(AVG(avg_price), 2) as segment_avg_price,
    ROUND(SUM(product_count) * 100.0 / (SELECT SUM(product_count) FROM cohort_analysis), 2) as market_share_percent
FROM cohort_analysis
GROUP BY performance_segment
ORDER BY total_products DESC;

-- 6. Advanced Business Intelligence Summary
-- =================================================================
WITH business_insights AS (
    SELECT 
        main_category,
        COUNT(*) as total_products,
        ROUND(AVG(CAST(ratings AS DECIMAL(3,2))), 2) as avg_rating,
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as avg_price,
        SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as total_customer_engagement,
        -- Business metrics
        COUNT(CASE WHEN CAST(ratings AS DECIMAL(3,2)) >= 4.0 THEN 1 END) as high_quality_products,
        COUNT(CASE WHEN CAST(REPLACE(no_of_ratings, ',', '') AS INT) >= 100 THEN 1 END) as established_products,
        -- Revenue potential (assuming review count correlates with sales)
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) * 
                  CAST(REPLACE(no_of_ratings, ',', '') AS INT)), 2) as revenue_potential_index
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
),
category_satisfaction AS (
    SELECT 
        main_category,
        satisfaction_level,
        review_volume_category,
        COUNT(*) as product_count,
        ROUND(AVG(rating), 2) as avg_rating,
        ROUND(AVG(price), 2) as avg_price,
        SUM(review_count) as total_reviews,
        -- Calculate satisfaction score weighted by review count
        ROUND(SUM(rating * review_count) / SUM(review_count), 2) as weighted_satisfaction_score
    FROM satisfaction_metrics
    GROUP BY main_category, satisfaction_level, review_volume_category
),
category_summary AS (
    SELECT 
        main_category,
        SUM(product_count) as total_products,
        SUM(CASE WHEN satisfaction_level IN ('Highly Satisfied', 'Satisfied') THEN product_count ELSE 0 END) as satisfied_products,
        ROUND(SUM(CASE WHEN satisfaction_level IN ('Highly Satisfied', 'Satisfied') THEN product_count ELSE 0 END) * 100.0 / SUM(product_count), 2) as satisfaction_rate,
        ROUND(AVG(weighted_satisfaction_score), 2) as overall_satisfaction_score
    FROM category_satisfaction
    GROUP BY main_category
)
SELECT 
    cs.main_category,
    cs.satisfaction_level,
    cs.review_volume_category,
    cs.product_count,
    cs.avg_rating,
    cs.avg_price,
    cs.total_reviews,
    cs.weighted_satisfaction_score,
    css.satisfaction_rate as category_satisfaction_rate,
    css.overall_satisfaction_score as category_overall_score
FROM category_satisfaction cs
JOIN category_summary css ON cs.main_category = css.main_category
WHERE cs.product_count >= 5  -- Focus on significant segments
ORDER BY cs.main_category, cs.satisfaction_level DESC, cs.review_volume_category DESC;

-- 5. Product Performance Cohort Analysis
-- =================================================================
WITH product_cohorts AS (
    SELECT 
        name,
        main_category,
        sub_category,
        CAST(ratings AS DECIMAL(3,2)) as rating,
        CAST(REPLACE(no_of_ratings, ',', '') AS INT) as review_count,
        CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) as price,
        -- Create cohorts based on performance metrics
        NTILE(4) OVER (ORDER BY CAST(ratings AS DECIMAL(3,2))) as rating_cohort,
        NTILE(4) OVER (ORDER BY CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as volume_cohort,
        NTILE(4) OVER (ORDER BY CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))) as price_cohort
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
),
cohort_analysis AS (
    SELECT 
        rating_cohort,
        volume_cohort,
        price_cohort,
        COUNT(*) as product_count,
        ROUND(AVG(rating), 2) as avg_rating,
        ROUND(AVG(review_count), 0) as avg_reviews,
        ROUND(AVG(price), 2) as avg_price,
        -- Performance classification
        CASE 
            WHEN rating_cohort = 4 AND volume_cohort = 4 THEN 'Stars'
            WHEN rating_cohort = 4 AND volume_cohort >= 3 THEN 'Rising Stars'
            WHEN rating_cohort >= 3 AND volume_cohort = 4 THEN 'Popular'
            WHEN rating_cohort >= 3 AND volume_cohort >= 3 THEN 'Solid Performers'
            WHEN rating_cohort <= 2 AND volume_cohort >= 3 THEN 'High Volume Low Rating'
            WHEN rating_cohort >= 3 AND volume_cohort <= 2 THEN 'Hidden Gems'
            ELSE 'Underperformers'
        END as performance_segment
    FROM product_cohorts
    GROUP BY rating_cohort, volume_cohort, price_cohort
)
SELECT 
    performance_segment,
    COUNT(*) as cohort_count,
    SUM(product_count) as total_products,
    ROUND(AVG(avg_rating), 2) as segment_avg_rating,
    ROUND(AVG(avg_reviews), 0) as segment_avg_reviews,
    ROUND(AVG(avg_price), 2) as segment_avg_price,
    ROUND(SUM(product_count) * 100.0 / (SELECT SUM(product_count) FROM cohort_analysis), 2) as market_share_percent
FROM cohort_analysis
GROUP BY performance_segment
ORDER BY total_products DESC;

-- 6. Advanced Business Intelligence Summary
-- =================================================================
WITH business_insights AS (
    SELECT 
        main_category,
        COUNT(*) as total_products,
        ROUND(AVG(CAST(ratings AS DECIMAL(3,2))), 2) as avg_rating,
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as avg_price,
        SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as total_customer_engagement,
        -- Business metrics
        COUNT(CASE WHEN CAST(ratings AS DECIMAL(3,2)) >= 4.0 THEN 1 END) as high_quality_products,
        COUNT(CASE WHEN CAST(REPLACE(no_of_ratings, ',', '') AS INT) >= 100 THEN 1 END) as established_products,
        -- Revenue potential (assuming review count correlates with sales)
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) * 
                  CAST(REPLACE(no_of_ratings, ',', '') AS INT)), 2) as revenue_potential_index
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
),
category_satisfaction AS (
    SELECT 
        main_category,
        satisfaction_level,
        review_volume_category,
        COUNT(*) as product_count,
        ROUND(AVG(rating), 2) as avg_rating,
        ROUND(AVG(price), 2) as avg_price,
        SUM(review_count) as total_reviews,
        -- Calculate satisfaction score weighted by review count
        ROUND(SUM(rating * review_count) / SUM(review_count), 2) as weighted_satisfaction_score
    FROM satisfaction_metrics
    GROUP BY main_category, satisfaction_level, review_volume_category
),
category_summary AS (
    SELECT 
        main_category,
        SUM(product_count) as total_products,
        SUM(CASE WHEN satisfaction_level IN ('Highly Satisfied', 'Satisfied') THEN product_count ELSE 0 END) as satisfied_products,
        ROUND(SUM(CASE WHEN satisfaction_level IN ('Highly Satisfied', 'Satisfied') THEN product_count ELSE 0 END) * 100.0 / SUM(product_count), 2) as satisfaction_rate,
        ROUND(AVG(weighted_satisfaction_score), 2) as overall_satisfaction_score
    FROM category_satisfaction
    GROUP BY main_category
)
SELECT 
    cs.main_category,
    cs.satisfaction_level,
    cs.review_volume_category,
    cs.product_count,
    cs.avg_rating,
    cs.avg_price,
    cs.total_reviews,
    cs.weighted_satisfaction_score,
    css.satisfaction_rate as category_satisfaction_rate,
    css.overall_satisfaction_score as category_overall_score
FROM category_satisfaction cs
JOIN category_summary css ON cs.main_category = css.main_category
WHERE cs.product_count >= 5  -- Focus on significant segments
ORDER BY cs.main_category, cs.satisfaction_level DESC, cs.review_volume_category DESC;

-- 5. Product Performance Cohort Analysis
-- =================================================================
WITH product_cohorts AS (
    SELECT 
        name,
        main_category,
        sub_category,
        CAST(ratings AS DECIMAL(3,2)) as rating,
        CAST(REPLACE(no_of_ratings, ',', '') AS INT) as review_count,
        CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) as price,
        -- Create cohorts based on performance metrics
        NTILE(4) OVER (ORDER BY CAST(ratings AS DECIMAL(3,2))) as rating_cohort,
        NTILE(4) OVER (ORDER BY CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as volume_cohort,
        NTILE(4) OVER (ORDER BY CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))) as price_cohort
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
),
cohort_analysis AS (
    SELECT 
        rating_cohort,
        volume_cohort,
        price_cohort,
        COUNT(*) as product_count,
        ROUND(AVG(rating), 2) as avg_rating,
        ROUND(AVG(review_count), 0) as avg_reviews,
        ROUND(AVG(price), 2) as avg_price,
        -- Performance classification
        CASE 
            WHEN rating_cohort = 4 AND volume_cohort = 4 THEN 'Stars'
            WHEN rating_cohort = 4 AND volume_cohort >= 3 THEN 'Rising Stars'
            WHEN rating_cohort >= 3 AND volume_cohort = 4 THEN 'Popular'
            WHEN rating_cohort >= 3 AND volume_cohort >= 3 THEN 'Solid Performers'
            WHEN rating_cohort <= 2 AND volume_cohort >= 3 THEN 'High Volume Low Rating'
            WHEN rating_cohort >= 3 AND volume_cohort <= 2 THEN 'Hidden Gems'
            ELSE 'Underperformers'
        END as performance_segment
    FROM product_cohorts
    GROUP BY rating_cohort, volume_cohort, price_cohort
)
SELECT 
    performance_segment,
    COUNT(*) as cohort_count,
    SUM(product_count) as total_products,
    ROUND(AVG(avg_rating), 2) as segment_avg_rating,
    ROUND(AVG(avg_reviews), 0) as segment_avg_reviews,
    ROUND(AVG(avg_price), 2) as segment_avg_price,
    ROUND(SUM(product_count) * 100.0 / (SELECT SUM(product_count) FROM cohort_analysis), 2) as market_share_percent
FROM cohort_analysis
GROUP BY performance_segment
ORDER BY total_products DESC;

-- 6. Advanced Business Intelligence Summary
-- =================================================================
WITH business_insights AS (
    SELECT 
        main_category,
        COUNT(*) as total_products,
        ROUND(AVG(CAST(ratings AS DECIMAL(3,2))), 2) as avg_rating,
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as avg_price,
        SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as total_customer_engagement,
        -- Business metrics
        COUNT(CASE WHEN CAST(ratings AS DECIMAL(3,2)) >= 4.0 THEN 1 END) as high_quality_products,
        COUNT(CASE WHEN CAST(REPLACE(no_of_ratings, ',', '') AS INT) >= 100 THEN 1 END) as established_products,
        -- Revenue potential (assuming review count correlates with sales)
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) * 
                  CAST(REPLACE(no_of_ratings, ',', '') AS INT)), 2) as revenue_potential_index
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    GROUP BY main_category, sub_category
    HAVING COUNT(*) >= 5
)
SELECT 
    main_category,
    market_opportunity,
    COUNT(*) as subcategory_count,
    ROUND(AVG(current_products), 0) as avg_products_per_subcategory,
    ROUND(AVG(avg_rating), 2) as avg_category_rating,
    ROUND(AVG(avg_price), 2) as avg_category_price,
    SUM(total_demand_signal) as total_category_demand
FROM market_gaps
WHERE market_opportunity != 'Saturated'
GROUP BY main_category, market_opportunity
ORDER BY total_category_demand DESC;
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
),
category_satisfaction AS (
    SELECT 
        main_category,
        satisfaction_level,
        review_volume_category,
        COUNT(*) as product_count,
        ROUND(AVG(rating), 2) as avg_rating,
        ROUND(AVG(price), 2) as avg_price,
        SUM(review_count) as total_reviews,
        -- Calculate satisfaction score weighted by review count
        ROUND(SUM(rating * review_count) / SUM(review_count), 2) as weighted_satisfaction_score
    FROM satisfaction_metrics
    GROUP BY main_category, satisfaction_level, review_volume_category
),
category_summary AS (
    SELECT 
        main_category,
        SUM(product_count) as total_products,
        SUM(CASE WHEN satisfaction_level IN ('Highly Satisfied', 'Satisfied') THEN product_count ELSE 0 END) as satisfied_products,
        ROUND(SUM(CASE WHEN satisfaction_level IN ('Highly Satisfied', 'Satisfied') THEN product_count ELSE 0 END) * 100.0 / SUM(product_count), 2) as satisfaction_rate,
        ROUND(AVG(weighted_satisfaction_score), 2) as overall_satisfaction_score
    FROM category_satisfaction
    GROUP BY main_category
)
SELECT 
    cs.main_category,
    cs.satisfaction_level,
    cs.review_volume_category,
    cs.product_count,
    cs.avg_rating,
    cs.avg_price,
    cs.total_reviews,
    cs.weighted_satisfaction_score,
    css.satisfaction_rate as category_satisfaction_rate,
    css.overall_satisfaction_score as category_overall_score
FROM category_satisfaction cs
JOIN category_summary css ON cs.main_category = css.main_category
WHERE cs.product_count >= 5  -- Focus on significant segments
ORDER BY cs.main_category, cs.satisfaction_level DESC, cs.review_volume_category DESC;

-- 5. Product Performance Cohort Analysis
-- =================================================================
WITH product_cohorts AS (
    SELECT 
        name,
        main_category,
        sub_category,
        CAST(ratings AS DECIMAL(3,2)) as rating,
        CAST(REPLACE(no_of_ratings, ',', '') AS INT) as review_count,
        CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) as price,
        -- Create cohorts based on performance metrics
        NTILE(4) OVER (ORDER BY CAST(ratings AS DECIMAL(3,2))) as rating_cohort,
        NTILE(4) OVER (ORDER BY CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as volume_cohort,
        NTILE(4) OVER (ORDER BY CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))) as price_cohort
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
),
cohort_analysis AS (
    SELECT 
        rating_cohort,
        volume_cohort,
        price_cohort,
        COUNT(*) as product_count,
        ROUND(AVG(rating), 2) as avg_rating,
        ROUND(AVG(review_count), 0) as avg_reviews,
        ROUND(AVG(price), 2) as avg_price,
        -- Performance classification
        CASE 
            WHEN rating_cohort = 4 AND volume_cohort = 4 THEN 'Stars'
            WHEN rating_cohort = 4 AND volume_cohort >= 3 THEN 'Rising Stars'
            WHEN rating_cohort >= 3 AND volume_cohort = 4 THEN 'Popular'
            WHEN rating_cohort >= 3 AND volume_cohort >= 3 THEN 'Solid Performers'
            WHEN rating_cohort <= 2 AND volume_cohort >= 3 THEN 'High Volume Low Rating'
            WHEN rating_cohort >= 3 AND volume_cohort <= 2 THEN 'Hidden Gems'
            ELSE 'Underperformers'
        END as performance_segment
    FROM product_cohorts
    GROUP BY rating_cohort, volume_cohort, price_cohort
)
SELECT 
    performance_segment,
    COUNT(*) as cohort_count,
    SUM(product_count) as total_products,
    ROUND(AVG(avg_rating), 2) as segment_avg_rating,
    ROUND(AVG(avg_reviews), 0) as segment_avg_reviews,
    ROUND(AVG(avg_price), 2) as segment_avg_price,
    ROUND(SUM(product_count) * 100.0 / (SELECT SUM(product_count) FROM cohort_analysis), 2) as market_share_percent
FROM cohort_analysis
GROUP BY performance_segment
ORDER BY total_products DESC;

-- 6. Advanced Business Intelligence Summary
-- =================================================================
WITH business_insights AS (
    SELECT 
        main_category,
        COUNT(*) as total_products,
        ROUND(AVG(CAST(ratings AS DECIMAL(3,2))), 2) as avg_rating,
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as avg_price,
        SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as total_customer_engagement,
        -- Business metrics
        COUNT(CASE WHEN CAST(ratings AS DECIMAL(3,2)) >= 4.0 THEN 1 END) as high_quality_products,
        COUNT(CASE WHEN CAST(REPLACE(no_of_ratings, ',', '') AS INT) >= 100 THEN 1 END) as established_products,
        -- Revenue potential (assuming review count correlates with sales)
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) * 
                  CAST(REPLACE(no_of_ratings, ',', '') AS INT)), 2) as revenue_potential_index
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'