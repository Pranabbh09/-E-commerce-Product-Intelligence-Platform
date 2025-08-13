# =================================================================
# AMAZON PRODUCTS ANALYSIS - PYSPARK IMPLEMENTATION
# Feature Engineering, Machine Learning, and Advanced Analytics
# =================================================================

import os
import glob
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("AmazonProductsAnalysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

class AmazonProductAnalyzer:
    def __init__(self, data_path):
        self.spark = spark
        self.data_path = data_path
        self.raw_df = None
        self.processed_df = None
        self.feature_df = None
        self.models = {}
        
    def load_data(self):
        """Load and combine all Amazon product CSV files"""
        print("Loading Amazon Products Dataset...")
        
        # Read all CSV files from the dataset
        csv_files = glob.glob(os.path.join(self.data_path, "*.csv"))
        
        if not csv_files:
            raise ValueError("No CSV files found in the specified path")
        
        # Read first file to get schema
        first_df = self.spark.read.csv(csv_files[0], header=True, inferSchema=True)
        
        # Read all files and union them
        all_dfs = [self.spark.read.csv(file, header=True, inferSchema=True) for file in csv_files]
        self.raw_df = all_dfs[0]
        
        for df in all_dfs[1:]:
            self.raw_df = self.raw_df.unionByName(df, allowMissingColumns=True)
        
        print(f"Dataset loaded: {self.raw_df.count()} products")
        return self
    
    def data_cleaning(self):
        """Comprehensive data cleaning and preprocessing"""
        print("Starting data cleaning...")
        
        # Remove duplicates
        self.processed_df = self.raw_df.dropDuplicates(['name', 'main_category', 'discount_price'])
        
        # Clean price columns - remove currency symbols and convert to numeric
        self.processed_df = self.processed_df.withColumn(
            'discount_price_clean',
            regexp_replace(regexp_replace(col('discount_price'), '[₹,]', ''), '\\s+', '').cast(FloatType())
        ).withColumn(
            'actual_price_clean',
            regexp_replace(regexp_replace(col('actual_price'), '[₹,]', ''), '\\s+', '').cast(FloatType())
        )
        
        # Clean ratings and review counts
        self.processed_df = self.processed_df.withColumn(
            'ratings_clean',
            when(col('ratings').rlike('^[0-5]\\.[0-9]$'), col('ratings').cast(FloatType()))
            .otherwise(None)
        ).withColumn(
            'no_of_ratings_clean',
            regexp_replace(col('no_of_ratings'), ',', '').cast(IntegerType())
        )
        
        # Filter out invalid records
        self.processed_df = self.processed_df.filter(
            (col('discount_price_clean').isNotNull()) &
            (col('ratings_clean').isNotNull()) &
            (col('no_of_ratings_clean').isNotNull()) &
            (col('main_category').isNotNull()) &
            (col('discount_price_clean') > 0) &
            (col('ratings_clean') >= 1) & 
            (col('ratings_clean') <= 5)
        )
        
        print(f"After cleaning: {self.processed_df.count()} products")
        return self
    
    def feature_engineering(self):
        """Advanced feature engineering"""
        print("Engineering features...")
        
        # Basic derived features
        self.feature_df = self.processed_df.withColumn(
            'discount_percentage',
            when(col('actual_price_clean').isNotNull() & (col('actual_price_clean') > col('discount_price_clean')),
                 ((col('actual_price_clean') - col('discount_price_clean')) / col('actual_price_clean') * 100))
            .otherwise(0)
        ).withColumn(
            'price_per_rating',
            col('discount_price_clean') / col('ratings_clean')
        ).withColumn(
            'review_density_score',
            when(col('no_of_ratings_clean') > 0, 
                 log(col('no_of_ratings_clean') + 1) * col('ratings_clean'))
            .otherwise(0)
        )
        
        # Price categorization
        price_percentiles = self.feature_df.select(
            expr('percentile_approx(discount_price_clean, 0.33)').alias('price_p33'),
            expr('percentile_approx(discount_price_clean, 0.66)').alias('price_p66')
        ).collect()[0]
        
        self.feature_df = self.feature_df.withColumn(
            'price_segment',
            when(col('discount_price_clean') <= price_percentiles['price_p33'], 'Budget')
            .when(col('discount_price_clean') <= price_percentiles['price_p66'], 'Mid-Range')
            .otherwise('Premium')
        )
        
        # Rating categorization
        self.feature_df = self.feature_df.withColumn(
            'rating_category',
            when(col('ratings_clean') >= 4.5, 'Excellent')
            .when(col('ratings_clean') >= 4.0, 'Good')
            .when(col('ratings_clean') >= 3.5, 'Average')
            .otherwise('Poor')
        )
        
        # Review volume categorization
        review_percentiles = self.feature_df.select(
            expr('percentile_approx(no_of_ratings_clean, 0.5)').alias('reviews_median'),
            expr('percentile_approx(no_of_ratings_clean, 0.8)').alias('reviews_p80')
        ).collect()[0]
        
        self.feature_df = self.feature_df.withColumn(
            'review_volume',
            when(col('no_of_ratings_clean') >= review_percentiles['reviews_p80'], 'High')
            .when(col('no_of_ratings_clean') >= review_percentiles['reviews_median'], 'Medium')
            .otherwise('Low')
        )
        
        # Window functions for advanced features
        category_window = Window.partitionBy('main_category')
        
        self.feature_df = self.feature_df.withColumn(
            'category_avg_rating',
            avg('ratings_clean').over(category_window)
        ).withColumn(
            'category_avg_price',
            avg('discount_price_clean').over(category_window)
        ).withColumn(
            'rating_vs_category_avg',
            col('ratings_clean') - col('category_avg_rating')
        ).withColumn(
            'price_vs_category_avg',
            (col('discount_price_clean') - col('category_avg_price')) / col('category_avg_price') * 100
        )
        
        # Popularity rank within category
        popularity_window = Window.partitionBy('main_category').orderBy(desc('no_of_ratings_clean'))
        rating_window = Window.partitionBy('main_category').orderBy(desc('ratings_clean'))
        
        self.feature_df = self.feature_df.withColumn(
            'popularity_rank',
            row_number().over(popularity_window)
        ).withColumn(
            'rating_rank',
            row_number().over(rating_window)
        )
        
        # Composite scores
        self.feature_df = self.feature_df.withColumn(
            'quality_score',
            (col('ratings_clean') * 0.6 + 
             (log(col('no_of_ratings_clean') + 1) / 10) * 0.4)
        ).withColumn(
            'value_score',
            col('quality_score') / (col('discount_price_clean') / 1000)
        )
        
        print("Feature engineering completed")
        return self
    
    def build_rating_prediction_model(self):
        """Build model to predict product ratings"""
        print("Building rating prediction model...")
        
        # Prepare features for ML
        string_indexer = StringIndexer(inputCol='main_category', outputCol='category_index')
        encoder = OneHotEncoder(inputCol='category_index', outputCol='category_encoded')
        
        feature_cols = ['discount_price_clean', 'no_of_ratings_clean', 'discount_percentage',
                       'category_encoded']
        assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
        scaler = StandardScaler(inputCol='features', outputCol='scaled_features')
        
        # Split data
        train_df, test_df = self.feature_df.randomSplit([0.8, 0.2], seed=42)
        
        # Random Forest Regressor
        rf = RandomForestRegressor(featuresCol='scaled_features', 
                                 labelCol='ratings_clean',
                                 numTrees=100,
                                 maxDepth=10)
        
        # Create pipeline
        pipeline = Pipeline(stages=[string_indexer, encoder, assembler, scaler, rf])
        
        # Train model
        self.models['rating_predictor'] = pipeline.fit(train_df)
        
        # Evaluate
        predictions = self.models['rating_predictor'].transform(test_df)
        evaluator = RegressionEvaluator(labelCol='ratings_clean', 
                                      predictionCol='prediction',
                                      metricName='rmse')
        rmse = evaluator.evaluate(predictions)
        print(f"Rating Prediction RMSE: {rmse:.3f}")
        
        return self
    
    def build_success_classification_model(self):
        """Build model to classify product success"""
        print("Building product success classification model...")
        
        # Define success criteria (high rating AND high review count)
        success_threshold_rating = 4.0
        success_threshold_reviews = self.feature_df.agg(
            expr('percentile_approx(no_of_ratings_clean, 0.7)')
        ).collect()[0][0]
        
        model_df = self.feature_df.withColumn(
            'is_successful',
            ((col('ratings_clean') >= success_threshold_rating) & 
             (col('no_of_ratings_clean') >= success_threshold_reviews)).cast(IntegerType())
        )
        
        # Feature preparation
        string_indexer = StringIndexer(inputCol='main_category', outputCol='category_index')
        price_indexer = StringIndexer(inputCol='price_segment', outputCol='price_segment_index')
        
        encoder1 = OneHotEncoder(inputCol='category_index', outputCol='category_encoded')
        encoder2 = OneHotEncoder(inputCol='price_segment_index', outputCol='price_segment_encoded')
        
        feature_cols = ['discount_price_clean', 'discount_percentage', 'price_vs_category_avg',
                       'category_encoded', 'price_segment_encoded']
        assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
        scaler = StandardScaler(inputCol='features', outputCol='scaled_features')
        
        # Split data
        train_df, test_df = model_df.randomSplit([0.8, 0.2], seed=42)
        
        # Logistic Regression
        lr = LogisticRegression(featuresCol='scaled_features', 
                              labelCol='is_successful',
                              maxIter=100)
        
        # Create pipeline
        pipeline = Pipeline(stages=[string_indexer, price_indexer, encoder1, encoder2, 
                                  assembler, scaler, lr])
        
        # Cross-validation
        paramGrid = ParamGridBuilder() \
            .addGrid(lr.regParam, [0.01, 0.1, 1.0]) \
            .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
            .build()
        
        crossval = CrossValidator(estimator=pipeline,
                                estimatorParamMaps=paramGrid,
                                evaluator=BinaryClassificationEvaluator(labelCol='is_successful'),
                                numFolds=5)
        
        # Train model
        self.models['success_classifier'] = crossval.fit(train_df)
        
        # Evaluate
        predictions = self.models['success_classifier'].transform(test_df)
        evaluator = BinaryClassificationEvaluator(labelCol='is_successful')
        auc = evaluator.evaluate(predictions)
        print(f"Success Classification AUC: {auc:.3f}")
        
        return self
    
    def product_clustering(self):
        """Perform customer segmentation using K-Means"""
        print("Performing product clustering...")
        
        # Prepare features for clustering
        feature_cols = ['discount_price_clean', 'ratings_clean', 'no_of_ratings_clean',
                       'discount_percentage', 'quality_score']
        assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
        scaler = StandardScaler(inputCol='features', outputCol='scaled_features')
        
        # Apply transformations
        temp_df = assembler.transform(self.feature_df)
        cluster_df = scaler.fit(temp_df).transform(temp_df)
        
        # K-Means clustering
        kmeans = KMeans(featuresCol='scaled_features', k=5, seed=42)
        self.models['kmeans'] = kmeans.fit(cluster_df)
        
        # Apply clustering
        clustered_df = self.models['kmeans'].transform(cluster_df)
        
        # Analyze clusters
        cluster_summary = clustered_df.groupBy('prediction').agg(
            count('*').alias('count'),
            avg('discount_price_clean').alias('avg_price'),
            avg('ratings_clean').alias('avg_rating'),
            avg('no_of_ratings_clean').alias('avg_reviews'),
            avg('discount_percentage').alias('avg_discount')
        ).orderBy('prediction')
        
        print("Cluster Analysis:")
        cluster_summary.show()
        
        # Store clustered data
        self.clustered_df = clustered_df
        
        return self
    
    def advanced_analytics(self):
        """Perform advanced analytics and generate insights"""
        print("Performing advanced analytics...")
        
        # Category performance analysis
        category_performance = self.feature_df.groupBy('main_category').agg(
            count('*').alias('product_count'),
            avg('ratings_clean').alias('avg_rating'),
            avg('discount_price_clean').alias('avg_price'),
            sum('no_of_ratings_clean').alias('total_reviews'),
            avg('quality_score').alias('avg_quality_score'),
            countDistinct('sub_category').alias('subcategory_diversity')
        ).orderBy(desc('avg_quality_score'))
        
        print("Category Performance Analysis:")
        category_performance.show(truncate=False)
        
        # Price elasticity analysis
        price_buckets = self.feature_df.withColumn(
            'price_bucket',
            when(col('discount_price_clean') < 500, 'Under 500')
            .when(col('discount_price_clean') < 1000, '500-1000')
            .when(col('discount_price_clean') < 2000, '1000-2000')
            .when(col('discount_price_clean') < 5000, '2000-5000')
            .otherwise('Above 5000')
        )
        
        price_analysis = price_buckets.groupBy('main_category', 'price_bucket').agg(
            count('*').alias('product_count'),
            avg('ratings_clean').alias('avg_rating'),
            avg('no_of_ratings_clean').alias('avg_reviews')
        ).orderBy('main_category', 'price_bucket')
        
        print("Price Elasticity Analysis:")
        price_analysis.show(truncate=False)
        
        # Market opportunity analysis
        market_gaps = self.feature_df.groupBy('main_category', 'sub_category').agg(
            count('*').alias('current_products'),
            avg('ratings_clean').alias('avg_rating'),
            sum('no_of_ratings_clean').alias('demand_signal')
        ).withColumn(
            'opportunity_score',
            col('demand_signal') / col('current_products')
        ).orderBy(desc('opportunity_score'))
        
        print("Market Opportunity Analysis (Top 20):")
        market_gaps.limit(20).show(truncate=False)
        
        # Feature importance analysis
        if 'rating_predictor' in self.models:
            feature_importance = self.models['rating_predictor'].stages[-1].featureImportances
            features = ['discount_price_clean', 'no_of_ratings_clean', 'discount_percentage']
            importance_dict = {features[i]: float(feature_importance[i]) for i in range(len(features))}
            print("Feature Importance for Rating Prediction:")
            for feature, importance in sorted(importance_dict.items(), key=lambda x: x[1], reverse=True):
                print(f"  {feature}: {importance:.4f}")
        
        return self
    
    def generate_business_insights(self):
        """Generate actionable business insights"""
        print("\n" + "="*50)
        print("BUSINESS INSIGHTS AND RECOMMENDATIONS")
        print("="*50)
        
        # Top performing categories
        top_categories = self.feature_df.groupBy('main_category').agg(
            avg('quality_score').alias('avg_quality'),
            count('*').alias('product_count')
        ).filter(col('product_count') >= 50).orderBy(desc('avg_quality'))
        
        print("\n1. TOP PERFORMING CATEGORIES:")
        top_categories.show(5, truncate=False)
        
        # Underperforming products with potential
        improvement_opportunities = self.feature_df.filter(
            (col('ratings_clean') < 4.0) & 
            (col('no_of_ratings_clean') > 100)
        ).select('name', 'main_category', 'ratings_clean', 'no_of_ratings_clean', 
                'discount_price_clean').orderBy(desc('no_of_ratings_clean'))
        
        print("\n2. IMPROVEMENT OPPORTUNITIES (High Volume, Low Rating):")
        improvement_opportunities.limit(10).show(truncate=False)
        
        # Pricing optimization candidates
        pricing_candidates = self.feature_df.filter(
            (col('ratings_clean') >= 4.5) & 
            (col('price_vs_category_avg') < -20)
        ).select('name', 'main_category', 'ratings_clean', 'discount_price_clean',
                'price_vs_category_avg').orderBy('price_vs_category_avg')
        
        print("\n3. UNDERPRICED HIGH-QUALITY PRODUCTS:")
        pricing_candidates.limit(10).show(truncate=False)
        
        return self
    
    def save_results(self, output_path):
        """Save processed data and model results"""
        print(f"Saving results to {output_path}")
        
        # Save processed dataset
        self.feature_df.coalesce(1).write.mode('overwrite').parquet(
            os.path.join(output_path, 'processed_data')
        )
        
        # Save model predictions
        if hasattr(self, 'clustered_df'):
            self.clustered_df.select(
                'name', 'main_category', 'ratings_clean', 'discount_price_clean',
                'quality_score', 'prediction'
            ).coalesce(1).write.mode('overwrite').parquet(
                os.path.join(output_path, 'product_clusters')
            )
        
        print("Results saved successfully")
        return self

# Main execution function
def main():
    # Configuration
    DATA_PATH = "/path/to/amazon/dataset"  # Update this path
    OUTPUT_PATH = "/path/to/output"        # Update this path
    
    # Initialize analyzer
    analyzer = AmazonProductAnalyzer(DATA_PATH)
    
    # Execute complete pipeline
    try:
        analyzer.load_data() \
               .data_cleaning() \
               .feature_engineering() \
               .build_rating_prediction_model() \
               .build_success_classification_model() \
               .product_clustering() \
               .advanced_analytics() \
               .generate_business_insights() \
               .save_results(OUTPUT_PATH)
        
        print("\nAnalysis completed successfully!")
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()