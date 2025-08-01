from typing import Dict, Any, List
from agents.tools.base_tool import BaseTool
from services.llm_interface import LLMInterface
import json


class AnalyzeCustomerSegmentsTool(BaseTool):
    """Tool for analyzing customer segments and providing insights"""
    
    def __init__(self, llm_service: LLMInterface, model: str):
        super().__init__(
            name="AnalyzeCustomerSegments",
            description="Analyze customer segment data and provide strategic insights"
        )
        self.llm_service = llm_service
        self.model = model
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "segment_data": {
                "type": "dict",
                "description": "Customer segmentation data from query results"
            },
            "analysis_focus": {
                "type": "string",
                "description": "Focus area: growth_opportunities, retention_strategies, product_recommendations, or general",
                "optional": True
            },
            "context": {
                "type": "dict",
                "description": "Additional context like business goals or constraints",
                "optional": True
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Analyze customer segments and provide insights"""
        segment_data = kwargs.get("segment_data", {})
        analysis_focus = kwargs.get("analysis_focus", "general")
        context = kwargs.get("context", {})
        
        try:
            # Prepare analysis prompt based on focus
            if analysis_focus == "growth_opportunities":
                analysis = self._analyze_growth_opportunities(segment_data, context)
            elif analysis_focus == "retention_strategies":
                analysis = self._analyze_retention_strategies(segment_data, context)
            elif analysis_focus == "product_recommendations":
                analysis = self._analyze_product_recommendations(segment_data, context)
            else:
                analysis = self._analyze_general_segments(segment_data, context)
            
            return {
                "success": True,
                "result": analysis
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "result": {}
            }
    
    def _analyze_growth_opportunities(self, data: Dict, context: Dict) -> Dict[str, Any]:
        """Analyze segments for growth opportunities"""
        prompt = f"""Analyze this customer segmentation data to identify growth opportunities:

Segmentation Data:
{json.dumps(data, indent=2)}

Context:
{json.dumps(context, indent=2) if context else 'No specific context provided'}

Provide a detailed analysis including:
1. High-value segments with expansion potential
2. Underserved segments that could be developed
3. Cross-sell and upsell opportunities by segment
4. Specific product recommendations for each segment
5. Estimated revenue impact of pursuing each opportunity

Focus on actionable insights with quantified potential where possible.

Respond with a JSON structure containing:
- opportunities: List of growth opportunities with details
- priority_segments: Top 3 segments to focus on
- quick_wins: Immediate actions that can be taken
- long_term_strategies: Strategic initiatives for sustained growth
- estimated_impact: Revenue/customer growth projections"""

        messages = [
            {"role": "system", "content": "You are a customer analytics expert specializing in growth strategy."},
            {"role": "user", "content": prompt}
        ]
        
        response = self.llm_service.complete(messages, model=self.model, temperature=0.3)
        
        try:
            analysis = json.loads(response)
        except json.JSONDecodeError:
            # Fallback structure
            analysis = {
                "opportunities": self._extract_growth_opportunities(data),
                "priority_segments": self._identify_priority_segments(data),
                "quick_wins": ["Enhance digital engagement", "Bundle product offerings", "Loyalty program enrollment"],
                "long_term_strategies": ["Segment-specific product development", "Personalized pricing strategies"],
                "estimated_impact": "10-15% revenue growth potential"
            }
        
        return analysis
    
    def _analyze_retention_strategies(self, data: Dict, context: Dict) -> Dict[str, Any]:
        """Analyze segments for retention strategies"""
        prompt = f"""Analyze this customer segmentation data to develop retention strategies:

Segmentation Data:
{json.dumps(data, indent=2)}

Context:
{json.dumps(context, indent=2) if context else 'No specific context provided'}

Provide a comprehensive retention analysis including:
1. At-risk segments and their characteristics
2. Churn indicators by segment
3. Targeted retention tactics for each segment
4. Early warning signals to monitor
5. Success metrics for retention programs

Focus on practical, implementable strategies.

Respond with a JSON structure containing:
- risk_assessment: Analysis of churn risk by segment
- retention_tactics: Specific strategies for each segment
- intervention_triggers: When to act on retention
- program_recommendations: Structured retention programs
- expected_outcomes: Impact on churn rates"""

        messages = [
            {"role": "system", "content": "You are a customer retention specialist with expertise in banking."},
            {"role": "user", "content": prompt}
        ]
        
        response = self.llm_service.complete(messages, model=self.model, temperature=0.3)
        
        try:
            analysis = json.loads(response)
        except json.JSONDecodeError:
            # Fallback structure
            analysis = {
                "risk_assessment": self._assess_retention_risks(data),
                "retention_tactics": {
                    "at_risk": ["Proactive outreach", "Special offers", "Service upgrades"],
                    "maintain": ["Regular check-ins", "Value reinforcement"],
                    "high_value": ["VIP treatment", "Exclusive benefits"]
                },
                "intervention_triggers": ["No activity for 60 days", "Balance decline >50%", "Product cancellation"],
                "program_recommendations": ["Loyalty rewards", "Relationship pricing", "Personal banker assignment"],
                "expected_outcomes": "20-30% reduction in churn rate"
            }
        
        return analysis
    
    def _analyze_product_recommendations(self, data: Dict, context: Dict) -> Dict[str, Any]:
        """Analyze segments for product recommendations"""
        prompt = f"""Analyze this customer segmentation data to generate product recommendations:

Segmentation Data:
{json.dumps(data, indent=2)}

Context:
{json.dumps(context, indent=2) if context else 'No specific context provided'}

Provide detailed product recommendations including:
1. Current product penetration by segment
2. Gap analysis - what products are underutilized
3. Next-best product recommendations by segment
4. Bundle opportunities
5. New product ideas based on segment needs

Consider customer lifecycle and financial capacity.

Respond with a JSON structure containing:
- current_penetration: Product adoption rates by segment
- gap_analysis: Underserved needs by segment
- recommendations: Specific product recommendations
- bundles: Attractive product combinations
- innovation_opportunities: New product ideas
- implementation_roadmap: Rollout strategy"""

        messages = [
            {"role": "system", "content": "You are a product strategy expert in retail banking."},
            {"role": "user", "content": prompt}
        ]
        
        response = self.llm_service.complete(messages, model=self.model, temperature=0.3)
        
        try:
            analysis = json.loads(response)
        except json.JSONDecodeError:
            # Fallback structure
            analysis = {
                "current_penetration": self._calculate_product_penetration(data),
                "gap_analysis": {
                    "high_value": "Low adoption of investment products",
                    "growth": "Missing savings/investment accounts",
                    "maintain": "Limited credit product usage"
                },
                "recommendations": self._generate_product_recommendations(data),
                "bundles": [
                    {"name": "Complete Checking", "products": ["Checking", "Savings", "Credit Card"]},
                    {"name": "Home Buyer Package", "products": ["Mortgage", "Checking", "Insurance"]}
                ],
                "innovation_opportunities": ["Digital wealth management", "Micro-investment products"],
                "implementation_roadmap": "Phase 1: High-value segment, Phase 2: Growth segment"
            }
        
        return analysis
    
    def _analyze_general_segments(self, data: Dict, context: Dict) -> Dict[str, Any]:
        """General segment analysis"""
        prompt = f"""Analyze this customer segmentation data comprehensively:

Segmentation Data:
{json.dumps(data, indent=2)}

Provide a thorough analysis including:
1. Segment characteristics and behaviors
2. Value distribution across segments
3. Key insights and patterns
4. Strategic recommendations
5. Areas requiring attention

Respond with a JSON structure containing:
- segment_profiles: Detailed characteristics of each segment
- value_analysis: Economic value by segment
- key_insights: Most important findings
- recommendations: Strategic actions to take
- watch_points: Areas needing monitoring"""

        messages = [
            {"role": "system", "content": "You are a senior banking analyst specializing in customer segmentation."},
            {"role": "user", "content": prompt}
        ]
        
        response = self.llm_service.complete(messages, model=self.model, temperature=0.3)
        
        try:
            analysis = json.loads(response)
        except json.JSONDecodeError:
            # Fallback structure
            analysis = self._create_general_analysis(data)
        
        return analysis
    
    def _extract_growth_opportunities(self, data: Dict) -> List[Dict]:
        """Extract growth opportunities from segment data"""
        opportunities = []
        
        if 'segments' in data:
            for segment in data['segments']:
                if segment.get('avg_products_per_customer', 0) < 2:
                    opportunities.append({
                        "segment": segment.get('segment'),
                        "opportunity": "Low product penetration",
                        "action": "Cross-sell campaign",
                        "potential": f"{segment.get('customer_count', 0) * 0.3:.0f} additional products"
                    })
        
        return opportunities
    
    def _identify_priority_segments(self, data: Dict) -> List[str]:
        """Identify priority segments based on value and growth potential"""
        priority = []
        
        if 'segments' in data:
            # Sort by total relationship value or customer count
            sorted_segments = sorted(
                data['segments'], 
                key=lambda x: x.get('avg_relationship_value', 0) * x.get('customer_count', 0),
                reverse=True
            )
            priority = [seg['segment'] for seg in sorted_segments[:3]]
        
        return priority
    
    def _assess_retention_risks(self, data: Dict) -> Dict[str, Any]:
        """Assess retention risks by segment"""
        risks = {}
        
        if 'segments' in data:
            for segment in data['segments']:
                if segment['segment'] == 'at_risk':
                    risks[segment['segment']] = {
                        "risk_level": "High",
                        "customer_count": segment.get('customer_count', 0),
                        "avg_value": segment.get('avg_relationship_value', 0),
                        "primary_factors": ["Low engagement", "Declining balances"]
                    }
                elif segment.get('avg_products_per_customer', 0) < 2:
                    risks[segment['segment']] = {
                        "risk_level": "Medium",
                        "customer_count": segment.get('customer_count', 0),
                        "primary_factors": ["Single product relationship"]
                    }
        
        return risks
    
    def _calculate_product_penetration(self, data: Dict) -> Dict[str, float]:
        """Calculate product penetration rates"""
        penetration = {}
        
        if 'segments' in data:
            for segment in data['segments']:
                penetration[segment['segment']] = {
                    "avg_products": segment.get('avg_products_per_customer', 0),
                    "penetration_rate": min(segment.get('avg_products_per_customer', 0) / 4.0 * 100, 100)
                }
        
        return penetration
    
    def _generate_product_recommendations(self, data: Dict) -> Dict[str, List[str]]:
        """Generate product recommendations by segment"""
        recommendations = {
            "high_value": ["Private banking services", "Investment products", "Premium credit cards"],
            "growth": ["Savings accounts", "Personal loans", "Basic investment products"],
            "maintain": ["Auto loans", "Home equity products", "CD accounts"],
            "at_risk": ["Consolidation loans", "Fee-free accounts", "Loyalty rewards"]
        }
        
        return recommendations
    
    def _create_general_analysis(self, data: Dict) -> Dict[str, Any]:
        """Create a general analysis structure"""
        total_customers = sum(seg.get('customer_count', 0) for seg in data.get('segments', []))
        
        return {
            "segment_profiles": {
                seg['segment']: {
                    "count": seg.get('customer_count', 0),
                    "avg_value": seg.get('avg_relationship_value', 0),
                    "characteristics": self._get_segment_characteristics(seg['segment'])
                }
                for seg in data.get('segments', [])
            },
            "value_analysis": {
                "total_customers": total_customers,
                "value_concentration": "High" if data.get('segments', []) else "Unknown"
            },
            "key_insights": [
                f"Total of {total_customers} customers across segments",
                "Opportunity for product expansion in growth segments",
                "At-risk segment requires immediate attention"
            ],
            "recommendations": [
                "Implement segment-specific marketing campaigns",
                "Develop retention program for at-risk customers",
                "Expand product offerings to growth segment"
            ],
            "watch_points": [
                "Monitor at-risk segment churn rates",
                "Track product adoption in growth segment"
            ]
        }
    
    def _get_segment_characteristics(self, segment: str) -> List[str]:
        """Get characteristics for a segment"""
        characteristics = {
            "high_value": ["High net worth", "Multiple products", "Long tenure", "High engagement"],
            "growth": ["Rising income", "Young professionals", "Digital-first", "Expansion potential"],
            "maintain": ["Stable income", "Moderate engagement", "Traditional banking preferences"],
            "at_risk": ["Low engagement", "Declining activity", "Single product", "Price sensitive"]
        }
        
        return characteristics.get(segment, ["General banking customer"])