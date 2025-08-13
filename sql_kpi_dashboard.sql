-- =================================================================
-- KPI DASHBOARD & BUSINESS INTELLIGENCE METRICS
-- Executive Dashboard Queries for Amazon Products Analysis
-- =================================================================

-- Executive Summary KPIs
-- =================================================================

-- 1. Key Performance Indicators (KPIs) Overview
WITH executive_kpis AS (
    SELECT 
        COUNT(*) as total_products,
        COUNT(DISTINCT main_category) as total_categories,
        COUNT(DISTINCT sub_category) as total_subcategories,
        ROUND(AVG(CAST(ratings AS DECIMAL(3,2))), 2) as overall_avg_rating,
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as overall_avg_price,
        SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as total_customer_reviews,
        COUNT(CASE WHEN CAST(ratings AS DECIMAL(3,2)) >= 4.0 THEN 1 END) as high_quality_products,
        COUNT(CASE WHEN CAST(REPLACE(no_of_ratings, ',', '') AS INT) >= 100 THEN 1 END) as established_products,
        COUNT(CASE WHEN CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) >= 2000 THEN 1 END) as premium_products
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
)
SELECT 
    'EXECUTIVE DASHBOARD - KEY METRICS' as dashboard_title,
    CONCAT('Total Products: ', FORMAT(total_products, 0)) as metric_1,
    CONCAT('Categories: ', total_categories, ' | Subcategories: ', total_subcategories) as metric_2,
    CONCAT('Avg Rating: ', overall_avg_rating, '/5.0') as metric_3,
    CONCAT('Avg Price: ₹', FORMAT(overall_avg_price, 0)) as metric_4,
    CONCAT('Total Reviews: ', FORMAT(total_customer_reviews, 0)) as metric_5,
    CONCAT('Quality Rate: ', ROUND(high_quality_products * 100.0 / total_products, 1), '%') as metric_6,
    CONCAT('Established Products: ', ROUND(established_products * 100.0 / total_products, 1), '%') as metric_7,
    CONCAT('Premium Products: ', ROUND(premium_products * 100.0 / total_products, 1), '%') as metric_8
FROM executive_kpis;

-- 2. Monthly/Trending KPIs (Simulated based on product performance)
-- =================================================================
WITH performance_trends AS (
    SELECT 
        main_category,
        COUNT(*) as product_count,
        ROUND(AVG(CAST(ratings AS DECIMAL(3,2))), 2) as avg_rating,
        SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as total_engagement,
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as avg_price,
        -- Growth indicators (simulated)
        CASE 
            WHEN AVG(CAST(ratings AS DECIMAL(3,2))) > 4.0 AND COUNT(*) > 50 THEN 'Growing'
            WHEN AVG(CAST(ratings AS DECIMAL(3,2))) > 3.5 AND COUNT(*) > 30 THEN 'Stable'
            ELSE 'Declining'
        END as trend_status,
        -- Market share approximation
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM amazon_products WHERE main_category IS NOT NULL), 2) as market_share
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
    GROUP BY main_category
    HAVING COUNT(*) >= 10
),
trending_metrics AS (
    SELECT 
        COUNT(CASE WHEN trend_status = 'Growing' THEN 1 END) as growing_categories,
        COUNT(CASE WHEN trend_status = 'Stable' THEN 1 END) as stable_categories,
        COUNT(CASE WHEN trend_status = 'Declining' THEN 1 END) as declining_categories,
        ROUND(AVG(CASE WHEN trend_status = 'Growing' THEN total_engagement END), 0) as avg_growing_engagement,
        ROUND(AVG(CASE WHEN trend_status = 'Growing' THEN avg_rating END), 2) as avg_growing_rating
    FROM performance_trends
)
SELECT 
    'TRENDING PERFORMANCE METRICS' as section,
    CONCAT('Growing Categories: ', growing_categories) as trend_1,
    CONCAT('Stable Categories: ', stable_categories) as trend_2,
    CONCAT('Declining Categories: ', declining_categories) as trend_3,
    CONCAT('Growth Engagement: ', FORMAT(avg_growing_engagement, 0)) as trend_4,
    CONCAT('Growth Quality: ', avg_growing_rating, '/5.0') as trend_5
FROM trending_metrics;

-- 3. Category Performance Dashboard
-- =================================================================
WITH category_performance AS (
    SELECT 
        main_category,
        COUNT(*) as total_products,
        ROUND(AVG(CAST(ratings AS DECIMAL(3,2))), 2) as avg_rating,
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as avg_price,
        SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as total_reviews,
        COUNT(CASE WHEN CAST(ratings AS DECIMAL(3,2)) >= 4.0 THEN 1 END) as quality_products,
        -- Revenue proxy calculation
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) * 
                  CAST(REPLACE(no_of_ratings, ',', '') AS INT) / 100), 2) as revenue_index,
        -- Performance scoring
        ROUND((AVG(CAST(ratings AS DECIMAL(3,2))) * 0.4 + 
               (LOG(SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) + 1) / 10) * 0.3 + 
               (COUNT(*) / 10) * 0.3), 2) as performance_score
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*$'
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+$'
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*$'
    GROUP BY main_category
    HAVING COUNT(*) >= 10
),
category_rankings AS (
    SELECT 
        *,
        ROUND(quality_products * 100.0 / total_products, 1) as quality_rate,
        RANK() OVER (ORDER BY performance_score DESC) as performance_rank,
        RANK() OVER (ORDER BY revenue_index DESC) as revenue_rank,
        RANK() OVER (ORDER BY total_reviews DESC) as engagement_rank,
        -- Strategic classification
        CASE 
            WHEN performance_score >= 4.0 AND revenue_index >= 1000 THEN 'Star'
            WHEN performance_score >= 3.5 AND total_reviews >= 10000 THEN 'Cash Cow'
            WHEN performance_score >= 3.5 AND total_products >= 50 THEN 'Question Mark'
            ELSE 'Dog'
        END as bcg_matrix
    FROM category_performance
)
SELECT 
    main_category,
    total_products,
    avg_rating,
    quality_rate,
    avg_price,
    total_reviews,
    revenue_index,
    performance_score,
    performance_rank,
    bcg_matrix,
    CASE 
        WHEN bcg_matrix = 'Star' THEN 'Maintain leadership, invest in growth'
        WHEN bcg_matrix = 'Cash Cow' THEN 'Optimize efficiency, milk profits'
        WHEN bcg_matrix = 'Question Mark' THEN 'Strategic decision: invest or divest'
        WHEN bcg_matrix = 'Dog' THEN 'Consider repositioning or exit'
    END as strategic_recommendation
FROM category_rankings
ORDER BY performance_rank;

-- 4. Pricing Intelligence Dashboard
-- =================================================================
WITH pricing_intelligence AS (
    SELECT 
        main_category,
        COUNT(*) as products,
        MIN(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))) as min_price,
        MAX(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))) as max_price,
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as avg_price,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as median_price,
        ROUND(STDDEV(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as price_stddev,
        -- Price-Quality correlation
        ROUND(AVG(CAST(ratings AS DECIMAL(3,2))), 2) as avg_rating,
        -- Price elasticity indicators
        COUNT(CASE WHEN CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) < 500 THEN 1 END) as budget_products,
        COUNT(CASE WHEN CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) >= 2000 THEN 1 END) as premium_products
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*
        
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*
        
    GROUP BY main_category
    HAVING COUNT(*) >= 15
),
price_positioning AS (
    SELECT 
        *,
        ROUND(budget_products * 100.0 / products, 1) as budget_share,
        ROUND(premium_products * 100.0 / products, 1) as premium_share,
        CASE 
            WHEN avg_price >= 2000 THEN 'Premium Market'
            WHEN avg_price >= 1000 THEN 'Mid Market'
            WHEN avg_price >= 500 THEN 'Mass Market'
            ELSE 'Budget Market'
        END as market_position,
        CASE 
            WHEN avg_rating >= 4.2 AND avg_price >= 1500 THEN 'Premium Quality'
            WHEN avg_rating >= 4.0 AND avg_price BETWEEN 500 AND 1500 THEN 'Value for Money'
            WHEN avg_rating >= 3.8 AND avg_price < 500 THEN 'Budget Quality'
            ELSE 'Price-Quality Gap'
        END as positioning_strategy
    FROM pricing_intelligence
)
SELECT 
    main_category,
    products,
    market_position,
    CONCAT('₹', FORMAT(min_price, 0), ' - ₹', FORMAT(max_price, 0)) as price_range,
    CONCAT('₹', FORMAT(avg_price, 0)) as avg_price,
    avg_rating,
    CONCAT(budget_share, '%') as budget_share,
    CONCAT(premium_share, '%') as premium_share,
    positioning_strategy,
    CASE 
        WHEN positioning_strategy = 'Premium Quality' THEN 'Maintain premium positioning'
        WHEN positioning_strategy = 'Value for Money' THEN 'Scale and optimize'
        WHEN positioning_strategy = 'Budget Quality' THEN 'Consider premium upgrades'
        ELSE 'Urgent pricing review needed'
    END as pricing_recommendation
FROM price_positioning
ORDER BY avg_price DESC;

-- 5. Customer Satisfaction & Quality Metrics
-- =================================================================
WITH satisfaction_metrics AS (
    SELECT 
        main_category,
        COUNT(*) as total_products,
        -- Rating distribution
        COUNT(CASE WHEN CAST(ratings AS DECIMAL(3,2)) >= 4.5 THEN 1 END) as excellent_products,
        COUNT(CASE WHEN CAST(ratings AS DECIMAL(3,2)) >= 4.0 AND CAST(ratings AS DECIMAL(3,2)) < 4.5 THEN 1 END) as good_products,
        COUNT(CASE WHEN CAST(ratings AS DECIMAL(3,2)) >= 3.5 AND CAST(ratings AS DECIMAL(3,2)) < 4.0 THEN 1 END) as average_products,
        COUNT(CASE WHEN CAST(ratings AS DECIMAL(3,2)) < 3.5 THEN 1 END) as poor_products,
        -- Engagement metrics
        ROUND(AVG(CAST(ratings AS DECIMAL(3,2))), 2) as avg_rating,
        SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as total_reviews,
        ROUND(AVG(CAST(REPLACE(no_of_ratings, ',', '') AS INT)), 0) as avg_reviews_per_product,
        -- Quality scores
        ROUND(SUM(CAST(ratings AS DECIMAL(3,2)) * CAST(REPLACE(no_of_ratings, ',', '') AS INT)) / 
              SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)), 2) as weighted_avg_rating
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*
        
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+
        
    GROUP BY main_category
    HAVING COUNT(*) >= 10
),
satisfaction_analysis AS (
    SELECT 
        *,
        ROUND(excellent_products * 100.0 / total_products, 1) as excellent_rate,
        ROUND((excellent_products + good_products) * 100.0 / total_products, 1) as satisfaction_rate,
        ROUND(poor_products * 100.0 / total_products, 1) as dissatisfaction_rate,
        -- Net Promoter Score approximation
        ROUND(((excellent_products * 100.0 / total_products) - (poor_products * 100.0 / total_products)), 1) as nps_approx,
        -- Customer engagement level
        CASE 
            WHEN avg_reviews_per_product >= 200 THEN 'High Engagement'
            WHEN avg_reviews_per_product >= 50 THEN 'Medium Engagement'
            ELSE 'Low Engagement'
        END as engagement_level
    FROM satisfaction_metrics
)
SELECT 
    main_category,
    total_products,
    CONCAT(satisfaction_rate, '%') as satisfaction_rate,
    CONCAT(excellent_rate, '%') as excellent_rate,
    avg_rating,
    weighted_avg_rating,
    FORMAT(total_reviews, 0) as total_reviews,
    engagement_level,
    nps_approx,
    CASE 
        WHEN satisfaction_rate >= 80 AND nps_approx >= 50 THEN 'Customer Champions'
        WHEN satisfaction_rate >= 70 AND nps_approx >= 30 THEN 'Customer Favorites'
        WHEN satisfaction_rate >= 60 AND nps_approx >= 10 THEN 'Customer Neutral'
        ELSE 'Customer Concern'
    END as customer_status,
    CASE 
        WHEN nps_approx >= 50 THEN 'Leverage for growth and referrals'
        WHEN nps_approx >= 30 THEN 'Focus on retention and upselling'
        WHEN nps_approx >= 10 THEN 'Quality improvement initiatives'
        ELSE 'Urgent customer experience overhaul'
    END as action_plan
FROM satisfaction_analysis
ORDER BY satisfaction_rate DESC;

-- 6. Revenue & Business Impact Analysis
-- =================================================================
WITH business_impact AS (
    SELECT 
        main_category,
        COUNT(*) as product_count,
        SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as customer_base_proxy,
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as avg_selling_price,
        -- Revenue indicators (reviews as proxy for sales volume)
        ROUND(SUM(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) * 
                  CAST(REPLACE(no_of_ratings, ',', '') AS INT) / 100), 2) as revenue_proxy,
        -- Market penetration
        ROUND(AVG(CAST(REPLACE(no_of_ratings, ',', '') AS INT)), 0) as avg_market_penetration,
        -- Quality-Price value
        ROUND(AVG(CAST(ratings AS DECIMAL(3,2)) / 
                  (CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) / 1000)), 3) as value_ratio
    FROM amazon_products
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*
        
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+
        
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*
        
    GROUP BY main_category
    HAVING COUNT(*) >= 15
),
revenue_analysis AS (
    SELECT 
        *,
        RANK() OVER (ORDER BY revenue_proxy DESC) as revenue_rank,
        RANK() OVER (ORDER BY customer_base_proxy DESC) as customer_rank,
        RANK() OVER (ORDER BY value_ratio DESC) as value_rank,
        -- Market share approximation
        ROUND(revenue_proxy * 100.0 / SUM(revenue_proxy) OVER (), 2) as revenue_share,
        -- Business classification
        CASE 
            WHEN revenue_proxy >= 100000 AND customer_base_proxy >= 50000 THEN 'Major Revenue Driver'
            WHEN revenue_proxy >= 50000 AND customer_base_proxy >= 20000 THEN 'Significant Contributor'
            WHEN revenue_proxy >= 20000 AND value_ratio >= 2.0 THEN 'High Value Niche'
            WHEN revenue_proxy >= 10000 THEN 'Growing Segment'
            ELSE 'Emerging Category'
        END as business_segment
    FROM business_impact
)
SELECT 
    main_category,
    business_segment,
    FORMAT(revenue_proxy, 0) as revenue_index,
    CONCAT(revenue_share, '%') as market_share,
    FORMAT(customer_base_proxy, 0) as customer_base,
    CONCAT('₹', FORMAT(avg_selling_price, 0)) as avg_price,
    value_ratio,
    revenue_rank,
    CASE 
        WHEN business_segment = 'Major Revenue Driver' THEN 'Strategic investment priority'
        WHEN business_segment = 'Significant Contributor' THEN 'Optimize and scale operations'
        WHEN business_segment = 'High Value Niche' THEN 'Premium positioning strategy'
        WHEN business_segment = 'Growing Segment' THEN 'Growth acceleration initiatives'
        ELSE 'Market development required'
    END as investment_strategy
FROM revenue_analysis
ORDER BY revenue_rank;

-- 7. Competitive Intelligence Dashboard
-- =================================================================
WITH competitive_landscape AS (
    SELECT 
        main_category,
        sub_category,
        COUNT(*) as competitors,
        ROUND(AVG(CAST(ratings AS DECIMAL(3,2))), 2) as category_avg_rating,
        ROUND(AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))), 2) as category_avg_price,
        MIN(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))) as lowest_price,
        MAX(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2))) as highest_price,
        SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as total_market_reviews,
        -- Market concentration (top 3 products' review share)
        (SELECT SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) 
         FROM (SELECT CAST(REPLACE(no_of_ratings, ',', '') AS INT) as reviews
               FROM amazon_products p2 
               WHERE p2.main_category = p1.main_category 
               AND p2.sub_category = p1.sub_category
               AND p2.no_of_ratings IS NOT NULL
               ORDER BY CAST(REPLACE(p2.no_of_ratings, ',', '') AS INT) DESC 
               LIMIT 3) top3) as top3_reviews
    FROM amazon_products p1
    WHERE ratings IS NOT NULL 
    AND ratings REGEXP '^[0-5]\\.?[0-9]*
        
    AND no_of_ratings IS NOT NULL
    AND no_of_ratings REGEXP '^[0-9,]+
        
    AND discount_price IS NOT NULL 
    AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*
        
    GROUP BY main_category, sub_category
    HAVING COUNT(*) >= 5
),
market_structure AS (
    SELECT 
        *,
        ROUND((highest_price - lowest_price) * 100.0 / category_avg_price, 1) as price_spread_percent,
        ROUND(top3_reviews * 100.0 / total_market_reviews, 1) as market_concentration,
        CASE 
            WHEN competitors >= 20 AND market_concentration <= 50 THEN 'Fragmented'
            WHEN competitors >= 10 AND market_concentration <= 70 THEN 'Competitive'
            WHEN competitors >= 5 AND market_concentration <= 85 THEN 'Concentrated'
            ELSE 'Monopolistic'
        END as market_structure_type,
        CASE 
            WHEN price_spread_percent <= 30 THEN 'Price Similarity'
            WHEN price_spread_percent <= 100 THEN 'Price Variance'
            ELSE 'Price Fragmentation'
        END as pricing_dynamics
    FROM competitive_landscape
)
SELECT 
    main_category,
    sub_category,
    competitors,
    market_structure_type,
    CONCAT(market_concentration, '%') as top3_dominance,
    category_avg_rating,
    CONCAT('₹', FORMAT(category_avg_price, 0)) as avg_market_price,
    CONCAT(price_spread_percent, '%') as price_variance,
    pricing_dynamics,
    CASE 
        WHEN market_structure_type = 'Fragmented' THEN 'Market leadership opportunity'
        WHEN market_structure_type = 'Competitive' THEN 'Differentiation strategy needed'
        WHEN market_structure_type = 'Concentrated' THEN 'Niche positioning required'
        ELSE 'Market disruption potential'
    END as competitive_strategy
FROM market_structure
WHERE total_market_reviews >= 1000  -- Focus on significant markets
ORDER BY total_market_reviews DESC;

-- 8. Executive Action Dashboard
-- =================================================================
WITH action_priorities AS (
    -- Revenue opportunities
    SELECT 
        'Revenue Optimization' as action_category,
        main_category as focus_area,
        COUNT(*) as opportunity_count,
        CONCAT('High-rated products priced below market: ', COUNT(*)) as description,
        'Immediate' as priority,
        'Price optimization review' as recommended_action
    FROM amazon_products
    WHERE CAST(ratings AS DECIMAL(3,2)) >= 4.5
    AND CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) < 
        (SELECT AVG(CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)))
         FROM amazon_products p2 WHERE p2.main_category = amazon_products.main_category)
    GROUP BY main_category
    HAVING COUNT(*) >= 5
    
    UNION ALL
    
    -- Quality improvement opportunities
    SELECT 
        'Quality Enhancement',
        main_category,
        COUNT(*),
        CONCAT('Products with high engagement but low ratings: ', COUNT(*)),
        'High',
        'Quality improvement program'
    FROM amazon_products
    WHERE CAST(ratings AS DECIMAL(3,2)) < 3.8
    AND CAST(REPLACE(no_of_ratings, ',', '') AS INT) > 100
    GROUP BY main_category
    HAVING COUNT(*) >= 3
    
    UNION ALL
    
    -- Market expansion opportunities
    SELECT 
        'Market Expansion',
        main_category,
        1,
        CONCAT('Underserved category with high demand signals'),
        'Medium',
        'Market entry strategy development'
    FROM (
        SELECT main_category, 
               COUNT(*) as product_count,
               SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) as demand
        FROM amazon_products
        WHERE ratings IS NOT NULL AND no_of_ratings IS NOT NULL
        GROUP BY main_category
        HAVING COUNT(*) < 30 AND SUM(CAST(REPLACE(no_of_ratings, ',', '') AS INT)) > 5000
    ) opportunities
)
SELECT 
    action_category,
    focus_area,
    opportunity_count,
    description,
    priority,
    recommended_action,
    CASE priority
        WHEN 'Immediate' THEN '🔴 Act within 30 days'
        WHEN 'High' THEN '🟡 Plan for next quarter'
        ELSE '🟢 Strategic planning cycle'
    END as timeline
FROM action_priorities
ORDER BY 
    CASE priority
        WHEN 'Immediate' THEN 1
        WHEN 'High' THEN 2
        ELSE 3
    END,
    opportunity_count DESC;

-- 9. Success Metrics Tracking Dashboard
-- =================================================================
WITH success_tracking AS (
    SELECT 
        'Product Portfolio Health' as metric_category,
        COUNT(*) as total_count,
        COUNT(CASE WHEN CAST(ratings AS DECIMAL(3,2)) >= 4.0 THEN 1 END) as success_count,
        ROUND(COUNT(CASE WHEN CAST(ratings AS DECIMAL(3,2)) >= 4.0 THEN 1 END) * 100.0 / COUNT(*), 1) as success_rate,
        'Products with 4+ rating' as metric_description
    FROM amazon_products
    WHERE ratings IS NOT NULL AND ratings REGEXP '^[0-5]\\.?[0-9]*
        
    
    UNION ALL
    
    SELECT 
        'Market Penetration',
        COUNT(*),
        COUNT(CASE WHEN CAST(REPLACE(no_of_ratings, ',', '') AS INT) >= 100 THEN 1 END),
        ROUND(COUNT(CASE WHEN CAST(REPLACE(no_of_ratings, ',', '') AS INT) >= 100 THEN 1 END) * 100.0 / COUNT(*), 1),
        'Products with 100+ reviews'
    FROM amazon_products
    WHERE no_of_ratings IS NOT NULL AND no_of_ratings REGEXP '^[0-9,]+
        
    
    UNION ALL
    
    SELECT 
        'Premium Positioning',
        COUNT(*),
        COUNT(CASE WHEN CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) >= 2000 THEN 1 END),
        ROUND(COUNT(CASE WHEN CAST(REPLACE(REPLACE(REPLACE(discount_price, '₹', ''), ',', ''), ' ', '') AS DECIMAL(10,2)) >= 2000 THEN 1 END) * 100.0 / COUNT(*), 1),
        'Premium products (₹2000+)'
    FROM amazon_products
    WHERE discount_price IS NOT NULL AND discount_price REGEXP '^₹?[0-9,]+\\.?[0-9]*
        
)
SELECT 
    metric_category,
    metric_description,
    FORMAT(total_count, 0) as total_products,
    FORMAT(success_count, 0) as target_achieved,
    CONCAT(success_rate, '%') as achievement_rate,
    CASE 
        WHEN success_rate >= 80 THEN '🟢 Excellent'
        WHEN success_rate >= 60 THEN '🟡 Good'
        WHEN success_rate >= 40 THEN '🟠 Fair'
        ELSE '🔴 Needs Improvement'
    END as performance_status
FROM success_tracking
ORDER BY success_rate DESC;

-- Final Summary: Executive Dashboard
SELECT 
    '🎯 AMAZON PRODUCTS ANALYTICS - EXECUTIVE SUMMARY' as dashboard_header,
    '' as separator1,
    '📊 Run individual sections above for detailed KPI analysis:' as instruction1,
    '   • Executive KPIs & Trending Metrics' as section1,
    '   • Category Performance & BCG Analysis' as section2,
    '   • Pricing Intelligence & Market Positioning' as section3,
    '   • Customer Satisfaction & Quality Metrics' as section4,
    '   • Revenue Analysis & Business Impact' as section5,
    '   • Competitive Intelligence Dashboard' as section6,
    '   • Executive Action Priorities' as section7,
    '   • Success Metrics Tracking' as section8,
    '' as separator2,
    '🚀 Use insights for strategic business decisions!' as conclusion;
        