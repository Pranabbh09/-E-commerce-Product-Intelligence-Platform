# Amazon Products Business Intelligence Dashboard
# Interactive Jupyter Notebook for Data Visualization and Analysis

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.figure_factory as ff
import warnings
warnings.filterwarnings('ignore')

# Set styling
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Configure plotly
import plotly.offline as pyo
pyo.init_notebook_mode(connected=True)

class AmazonDashboard:
    def __init__(self, data_path):
        """Initialize dashboard with data loading and preprocessing"""
        self.data_path = data_path
        self.df = None
        self.processed_df = None
        self.load_and_process_data()
    
    def load_and_process_data(self):
        """Load and preprocess Amazon products data"""
        print("Loading Amazon Products Dataset...")
        
        # Load data (assuming CSV format)
        self.df = pd.read_csv(self.data_path)
        
        # Data cleaning
        self.processed_df = self.df.copy()
        
        # Clean price columns
        self.processed_df['discount_price_clean'] = (
            self.processed_df['discount_price']
            .str.replace('[₹,]', '', regex=True)
            .str.replace(' ', '')
            .astype(float, errors='ignore')
        )
        
        self.processed_df['actual_price_clean'] = (
            self.processed_df['actual_price']
            .str.replace('[₹,]', '', regex=True)
            .str.replace(' ', '')
            .astype(float, errors='ignore')
        )
        
        # Clean ratings
        self.processed_df['ratings_clean'] = pd.to_numeric(
            self.processed_df['ratings'], errors='coerce'
        )
        
        # Clean review counts
        self.processed_df['no_of_ratings_clean'] = (
            self.processed_df['no_of_ratings']
            .str.replace(',', '')
            .astype(int, errors='ignore')
        )
        
        # Calculate derived metrics
        self.processed_df['discount_percentage'] = np.where(
            self.processed_df['actual_price_clean'] > self.processed_df['discount_price_clean'],
            ((self.processed_df['actual_price_clean'] - self.processed_df['discount_price_clean']) / 
             self.processed_df['actual_price_clean'] * 100),
            0
        )
        
        # Quality score
        self.processed_df['quality_score'] = (
            self.processed_df['ratings_clean'] * 0.6 + 
            (np.log(self.processed_df['no_of_ratings_clean'] + 1) / 10) * 0.4
        )
        
        # Filter valid data
        self.processed_df = self.processed_df.dropna(subset=[
            'discount_price_clean', 'ratings_clean', 'no_of_ratings_clean'
        ])
        
        print(f"Data loaded and processed: {len(self.processed_df)} products")
        
    def create_overview_dashboard(self):
        """Create executive overview dashboard"""
        print("Creating Executive Overview Dashboard...")
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Products by Category', 'Rating Distribution', 
                          'Price vs Rating Scatter', 'Top Categories by Quality Score'),
            specs=[[{"type": "bar"}, {"type": "histogram"}],
                   [{"type": "scatter"}, {"type": "bar"}]]
        )
        
        # 1. Products by Category
        category_counts = self.processed_df['main_category'].value_counts().head(10)
        fig.add_trace(
            go.Bar(x=category_counts.index, y=category_counts.values, 
                   name='Product Count', marker_color='lightblue'),
            row=1, col=1
        )
        
        # 2. Rating Distribution
        fig.add_trace(
            go.Histogram(x=self.processed_df['ratings_clean'], nbinsx=20,
                        name='Ratings', marker_color='lightgreen'),
            row=1, col=2
        )
        
        # 3. Price vs Rating Scatter
        sample_df = self.processed_df.sample(n=min(1000, len(self.processed_df)))
        fig.add_trace(
            go.Scatter(x=sample_df['discount_price_clean'], 
                      y=sample_df['ratings_clean'],
                      mode='markers', name='Products',
                      marker=dict(color=sample_df['no_of_ratings_clean'],
                                colorscale='viridis', showscale=True)),
            row=2, col=1
        )
        
        # 4. Top Categories by Quality Score
        quality_by_category = (self.processed_df.groupby('main_category')['quality_score']
                              .mean().sort_values(ascending=False).head(10))
        fig.add_trace(
            go.Bar(x=quality_by_category.index, y=quality_by_category.values,
                   name='Quality Score', marker_color='coral'),
            row=2, col=2
        )
        
        # Update layout
        fig.update_layout(
            height=800, 
            title_text="Amazon Products - Executive Overview Dashboard",
            showlegend=False
        )
        
        fig.show()
        
    def create_category_analysis(self):
        """Detailed category performance analysis"""
        print("Creating Category Analysis Dashboard...")
        
        # Category metrics
        category_metrics = self.processed_df.groupby('main_category').agg({
            'ratings_clean': ['mean', 'count'],
            'discount_price_clean': 'mean',
            'no_of_ratings_clean': 'sum',
            'quality_score': 'mean',
            'discount_percentage': 'mean'
        }).round(2)
        
        category_metrics.columns = ['avg_rating', 'product_count', 'avg_price', 
                                   'total_reviews', 'quality_score', 'avg_discount']
        category_metrics = category_metrics.reset_index()
        
        # Create interactive bubble chart
        fig = px.scatter(category_metrics, 
                        x='avg_price', y='avg_rating',
                        size='total_reviews', color='quality_score',
                        hover_name='main_category',
                        hover_data=['product_count', 'avg_discount'],
                        title='Category Performance Analysis - Bubble Chart',
                        labels={
                            'avg_price': 'Average Price (₹)',
                            'avg_rating': 'Average Rating',
                            'total_reviews': 'Total Reviews',
                            'quality_score': 'Quality Score'
                        })
        fig.update_layout(height=600)
        fig.show()
        
        # Category comparison heatmap
        metrics_for_heatmap = category_metrics.set_index('main_category')[
            ['avg_rating', 'avg_price', 'quality_score', 'avg_discount']
        ]
        
        # Normalize for better visualization
        metrics_normalized = (metrics_for_heatmap - metrics_for_heatmap.min()) / (
            metrics_for_heatmap.max() - metrics_for_heatmap.min()
        )
        
        fig2 = px.imshow(metrics_normalized.T, 
                        title='Category Performance Heatmap (Normalized)',
                        aspect='auto', color_continuous_scale='RdYlBu_r')
        fig2.update_layout(height=500)
        fig2.show()
        
    def create_pricing_analysis(self):
        """Pricing strategy analysis dashboard"""
        print("Creating Pricing Analysis Dashboard...")
        
        # Price segmentation
        self.processed_df['price_segment'] = pd.cut(
            self.processed_df['discount_price_clean'],
            bins=[0, 500, 1000, 2000, 5000, float('inf')],
            labels=['Budget', 'Economy', 'Mid-Range', 'Premium', 'Luxury']
        )
        
        # Create pricing analysis plots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Price Distribution by Category', 'Rating vs Price Segment',
                          'Discount Impact on Ratings', 'Price Elasticity Analysis'),
            specs=[[{"type": "box"}, {"type": "bar"}],
                   [{"type": "scatter"}, {"type": "violin"}]]
        )
        
        # 1. Price distribution by top categories
        top_categories = self.processed_df['main_category'].value_counts().head(5).index
        for i, category in enumerate(top_categories):
            category_data = self.processed_df[
                self.processed_df['main_category'] == category
            ]['discount_price_clean']
            fig.add_trace(
                go.Box(y=category_data, name=category, showlegend=False),
                row=1, col=1
            )
        
        # 2. Rating vs Price Segment
        rating_by_segment = self.processed_df.groupby('price_segment')['ratings_clean'].mean()
        fig.add_trace(
            go.Bar(x=rating_by_segment.index, y=rating_by_segment.values,
                   name='Avg Rating', showlegend=False, marker_color='lightcoral'),
            row=1, col=2
        )
        
        # 3. Discount Impact
        discount_sample = self.processed_df.sample(n=min(500, len(self.processed_df)))
        fig.add_trace(
            go.Scatter(x=discount_sample['discount_percentage'],
                      y=discount_sample['ratings_clean'],
                      mode='markers', name='Products', showlegend=False,
                      marker=dict(color='blue', opacity=0.6)),
            row=2, col=1
        )
        
        # 4. Price distribution violin
        sample_categories = self.processed_df['main_category'].value_counts().head(3).index
        for category in sample_categories:
            category_prices = self.processed_df[
                self.processed_df['main_category'] == category
            ]['discount_price_clean']
            fig.add_trace(
                go.Violin(y=category_prices, name=category, showlegend=False),
                row=2, col=2
            )
        
        fig.update_layout(height=800, title_text="Pricing Strategy Analysis Dashboard")
        fig.show()
        
        # Price optimization recommendations
        print("\n📊 PRICING OPTIMIZATION INSIGHTS:")
        
        # High-rated, low-priced products (underpriced)
        underpriced = self.processed_df[
            (self.processed_df['ratings_clean'] >= 4.5) &
            (self.processed_df['discount_price_clean'] <= 
             self.processed_df.groupby('main_category')['discount_price_clean'].transform('median'))
        ].nlargest(10, 'no_of_ratings_clean')[
            ['name', 'main_category', 'ratings_clean', 'discount_price_clean', 'no_of_ratings_clean']
        ]
        
        print("\nTop Underpriced Products (High Rating, Low Price):")
        print(underpriced.to_string(index=False))
        
    def create_performance_dashboard(self):
        """Product performance and success factors analysis"""
        print("Creating Performance Analysis Dashboard...")
        
        # Define success criteria
        high_rating_threshold = 4.0
        high_review_threshold = self.processed_df['no_of_ratings_clean'].quantile(0.7)
        
        self.processed_df['is_successful'] = (
            (self.processed_df['ratings_clean'] >= high_rating_threshold) &
            (self.processed_df['no_of_ratings_clean'] >= high_review_threshold)
        )
        
        # Success rate by category
        success_by_category = (
            self.processed_df.groupby('main_category')['is_successful']
            .agg(['sum', 'count'])
            .assign(success_rate=lambda x: x['sum'] / x['count'] * 100)
            .sort_values('success_rate', ascending=False)
            .head(10)
        )
        
        # Create performance dashboard
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Success Rate by Category', 'Rating vs Review Volume',
                          'Performance Quadrant Analysis', 'Success Factors'),
            specs=[[{"type": "bar"}, {"type": "scatter"}],
                   [{"type": "scatter"}, {"type": "bar"}]]
        )
        
        # 1. Success rate by category
        fig.add_trace(
            go.Bar(x=success_by_category.index, y=success_by_category['success_rate'],
                   name='Success Rate (%)', marker_color='green'),
            row=1, col=1
        )
        
        # 2. Rating vs Review Volume
        sample_data = self.processed_df.sample(n=min(1000, len(self.processed_df)))
        fig.add_trace(
            go.Scatter(x=sample_data['no_of_ratings_clean'],
                      y=sample_data['ratings_clean'],
                      mode='markers',
                      marker=dict(
                          color=sample_data['is_successful'].map({True: 'green', False: 'red'}),
                          opacity=0.6
                      ),
                      name='Products'),
            row=1, col=2
        )
        
        # 3. Performance Quadrant Analysis
        fig.add_trace(
            go.Scatter(x=sample_data['discount_price_clean'],
                      y=sample_data['quality_score'],
                      mode='markers',
                      marker=dict(
                          color=sample_data['discount_percentage'],
                          colorscale='viridis',
                          showscale=True
                      ),
                      name='Quality vs Price'),
            row=2, col=1
        )
        
        # 4. Success factors
        successful_products = self.processed_df[self.processed_df['is_successful']]
        unsuccessful_products = self.processed_df[~self.processed_df['is_successful']]
        
        factors = ['discount_percentage', 'quality_score']
        successful_means = [successful_products[factor].mean() for factor in factors]
        unsuccessful_means = [unsuccessful_products[factor].mean() for factor in factors]
        
        fig.add_trace(
            go.Bar(x=factors, y=successful_means, name='Successful', marker_color='lightgreen'),
            row=2, col=2
        )
        fig.add_trace(
            go.Bar(x=factors, y=unsuccessful_means, name='Unsuccessful', marker_color='lightcoral'),
            row=2, col=2
        )
        
        fig.update_layout(height=800, title_text="Product Performance Analysis Dashboard")
        fig.show()
        
        # Performance insights
        print("\n🎯 PERFORMANCE INSIGHTS:")
        print(f"Overall Success Rate: {self.processed_df['is_successful'].mean():.1%}")
        print(f"Average Rating of Successful Products: {successful_products['ratings_clean'].mean():.2f}")
        print(f"Average Reviews of Successful Products: {successful_products['no_of_ratings_clean'].mean():.0f}")
        
    def create_market_opportunity_analysis(self):
        """Market gaps and opportunities analysis"""
        print("Creating Market Opportunity Analysis...")
        
        # Category-subcategory analysis
        market_analysis = self.processed_df.groupby(['main_category', 'sub_category']).agg({
            'ratings_clean': 'count',
            'ratings_clean': 'mean',
            'no_of_ratings_clean': 'sum',
            'discount_price_clean': 'mean'
        }).round(2)
        
        market_analysis.columns = ['product_count', 'avg_rating', 'demand_signal', 'avg_price']
        market_analysis = market_analysis.reset_index()
        
        # Identify opportunities
        market_analysis['opportunity_score'] = (
            market_analysis['demand_signal'] / market_analysis['product_count']
        )
        
        # Market gaps (high demand, low supply)
        market_gaps = market_analysis[
            (market_analysis['product_count'] < 20) &
            (market_analysis['demand_signal'] > 500)
        ].nlargest(15, 'opportunity_score')
        
        # Visualization
        fig = px.scatter(market_analysis,
                        x='product_count', y='avg_rating',
                        size='demand_signal', color='opportunity_score',
                        hover_name='sub_category',
                        hover_data=['main_category', 'avg_price'],
                        title='Market Opportunity Analysis - Subcategory Level',
                        labels={
                            'product_count': 'Number of Products',
                            'avg_rating': 'Average Rating',
                            'demand_signal': 'Total Reviews',
                            'opportunity_score': 'Opportunity Score'
                        })
        fig.update_layout(height=700)
        fig.show()
        
        # Top opportunities table
        print("\n🚀 TOP MARKET OPPORTUNITIES:")
        print("High Demand, Low Competition Subcategories:")
        print(market_gaps[['main_category', 'sub_category', 'product_count', 
                          'avg_rating', 'demand_signal', 'opportunity_score']].to_string(index=False))
        
    def generate_executive_summary(self):
        """Generate executive summary with key metrics and insights"""
        print("="*60)
        print("📈 AMAZON PRODUCTS - EXECUTIVE SUMMARY")
        print("="*60)
        
        # Key metrics
        total_products = len(self.processed_df)
        total_categories = self.processed_df['main_category'].nunique()
        avg_rating = self.processed_df['ratings_clean'].mean()
        avg_price = self.processed_df['discount_price_clean'].mean()
        total_reviews = self.processed_df['no_of_ratings_clean'].sum()
        
        print(f"\n📊 KEY METRICS:")
        print(f"Total Products Analyzed: {total_products:,}")
        print(f"Product Categories: {total_categories}")
        print(f"Average Product Rating: {avg_rating:.2f}/5.0")
        print(f"Average Product Price: ₹{avg_price:,.0f}")
        print(f"Total Customer Reviews: {total_reviews:,}")
        
        # Top performers
        top_categories = (self.processed_df.groupby('main_category')['quality_score']
                         .mean().nlargest(5))
        print(f"\n🏆 TOP PERFORMING CATEGORIES:")
        for i, (category, score) in enumerate(top_categories.items(), 1):
            print(f"{i}. {category}: {score:.2f}")
        
        # Market insights
        high_potential = self.processed_df[
            (self.processed_df['ratings_clean'] >= 4.5) &
            (self.processed_df['discount_price_clean'] <= 
             self.processed_df['discount_price_clean'].median())
        ]
        
        print(f"\n💡 STRATEGIC INSIGHTS:")
        print(f"High-Quality, Low-Price Products: {len(high_potential)} ({len(high_potential)/total_products:.1%})")
        print(f"Products with 4+ Rating: {len(self.processed_df[self.processed_df['ratings_clean'] >= 4])}")
        print(f"Premium Products (>₹2000): {len(self.processed_df[self.processed_df['discount_price_clean'] > 2000])}")
        
        # Recommendations
        print(f"\n🎯 KEY RECOMMENDATIONS:")
        print("1. Focus on underpriced high-quality products for revenue optimization")
        print("2. Invest in quality improvement for categories with high volume but low ratings")
        print("3. Explore market gaps in high-demand, low-competition subcategories")
        print("4. Develop premium product lines in successful categories")
        
    def run_complete_analysis(self):
        """Run the complete dashboard analysis"""
        print("🚀 Starting Complete Amazon Products Analysis...")
        
        # Execute all dashboard components
        self.create_overview_dashboard()
        self.create_category_analysis()
        self.create_pricing_analysis()
        self.create_performance_dashboard()
        self.create_market_opportunity_analysis()
        self.generate_executive_summary()
        
        print("\n✅ Analysis Complete! All dashboards have been generated.")

# Usage Instructions and Main Execution
if __name__ == "__main__":
    print("""
    🔥 AMAZON PRODUCTS BUSINESS INTELLIGENCE DASHBOARD 🔥
    
    This notebook provides comprehensive analysis of Amazon products data including:
    - Executive Overview Dashboard
    - Category Performance Analysis  
    - Pricing Strategy Analysis
    - Product Performance Metrics
    - Market Opportunity Analysis
    - Executive Summary with Insights
    
    Instructions:
    1. Update the data_path below to point to your Amazon products CSV file
    2. Run all cells to generate interactive dashboards
    3. Use the insights for business decision making
    """)
    
    # Configuration - UPDATE THIS PATH
    DATA_PATH = "amazon_products.csv"  # Update with your data file path
    
    try:
        # Initialize and run dashboard
        dashboard = AmazonDashboard(DATA_PATH)
        dashboard.run_complete_analysis()
        
    except FileNotFoundError:
        print(f"❌ Error: Data file not found at {DATA_PATH}")
        print("Please update DATA_PATH with the correct path to your Amazon products CSV file")
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")

# Additional utility functions for advanced analysis
def create_custom_analysis(df, category_filter=None, price_range=None):
    """Create custom analysis for specific categories or price ranges"""
    filtered_df = df.copy()
    
    if category_filter:
        filtered_df = filtered_df[filtered_df['main_category'].isin(category_filter)]
    
    if price_range:
        filtered_df = filtered_df[
            (filtered_df['discount_price_clean'] >= price_range[0]) &
            (filtered_df['discount_price_clean'] <= price_range[1])
        ]
    
    # Create custom visualizations
    fig = px.scatter_matrix(
        filtered_df[['ratings_clean', 'discount_price_clean', 'no_of_ratings_clean', 'quality_score']],
        title="Custom Analysis - Correlation Matrix"
    )
    fig.show()
    
    return filtered_df

# Interactive widgets for Jupyter notebook (optional)
try:
    from ipywidgets import interact, widgets
    
    def interactive_category_analysis(category):
        """Interactive category analysis with widgets"""
        filtered_data = dashboard.processed_df[
            dashboard.processed_df['main_category'] == category
        ]
        
        print(f"\n📊 Analysis for {category}:")
        print(f"Products: {len(filtered_data)}")
        print(f"Average Rating: {filtered_data['ratings_clean'].mean():.2f}")
        print(f"Average Price: ₹{filtered_data['discount_price_clean'].mean():.0f}")
        
        # Create quick visualization
        fig = px.histogram(filtered_data, x='ratings_clean', 
                          title=f'Rating Distribution - {category}')
        fig.show()
    
    # Widget setup (uncomment to use)
    # categories_widget = widgets.Dropdown(
    #     options=dashboard.processed_df['main_category'].unique(),
    #     description='Category:'
    # )
    # interact(interactive_category_analysis, category=categories_widget)
    
except ImportError:
    print("ipywidgets not available - skipping interactive features")

print("📝 Notebook setup complete! Run dashboard.run_complete_analysis() to start.")