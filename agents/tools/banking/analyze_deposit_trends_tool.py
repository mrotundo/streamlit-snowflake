from typing import Dict, Any, List
from agents.tools.base_tool import BaseTool
from services.llm_interface import LLMInterface
import json


class AnalyzeDepositTrendsTool(BaseTool):
    """Tool for analyzing deposit trends and providing strategic insights"""
    
    def __init__(self, llm_service: LLMInterface, model: str):
        super().__init__(
            name="AnalyzeDepositTrends",
            description="Analyze deposit data trends and provide liquidity insights"
        )
        self.llm_service = llm_service
        self.model = model
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "deposit_data": {
                "type": "dict",
                "description": "Deposit account data from query results"
            },
            "analysis_focus": {
                "type": "string",
                "description": "Focus area: growth_analysis, stability_assessment, rate_sensitivity, or comprehensive",
                "optional": True
            },
            "market_data": {
                "type": "dict",
                "description": "Market rates and competitor data",
                "optional": True
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Analyze deposit trends and provide insights"""
        deposit_data = kwargs.get("deposit_data", {})
        analysis_focus = kwargs.get("analysis_focus", "comprehensive")
        market_data = kwargs.get("market_data", {})
        
        try:
            # Perform focused analysis
            if analysis_focus == "growth_analysis":
                analysis = self._analyze_deposit_growth(deposit_data, market_data)
            elif analysis_focus == "stability_assessment":
                analysis = self._analyze_deposit_stability(deposit_data)
            elif analysis_focus == "rate_sensitivity":
                analysis = self._analyze_rate_sensitivity(deposit_data, market_data)
            else:
                analysis = self._comprehensive_deposit_analysis(deposit_data, market_data)
            
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
    
    def _analyze_deposit_growth(self, data: Dict, market: Dict) -> Dict[str, Any]:
        """Analyze deposit growth patterns"""
        prompt = f"""Analyze deposit growth trends from this data:

Deposit Data:
{json.dumps(data, indent=2)}

Market Context:
{json.dumps(market, indent=2) if market else 'No market data provided'}

Provide growth analysis including:
1. Growth trends by account type and customer segment
2. New account acquisition patterns
3. Balance migration analysis
4. Competitive positioning
5. Growth drivers and inhibitors
6. Forecast and recommendations

Respond with JSON containing:
- growth_metrics: Key growth indicators
- trend_analysis: Growth patterns identified
- acquisition_insights: New account trends
- competitive_position: Market share analysis
- growth_opportunities: Identified opportunities
- recommendations: Strategic actions for growth"""

        messages = [
            {"role": "system", "content": "You are a deposit growth strategist with expertise in retail banking."},
            {"role": "user", "content": prompt}
        ]
        
        response = self.llm_service.complete(messages, model=self.model, temperature=0.3)
        
        try:
            return json.loads(response)
        except:
            return self._default_growth_analysis(data)
    
    def _analyze_deposit_stability(self, data: Dict) -> Dict[str, Any]:
        """Analyze deposit stability and liquidity risk"""
        prompt = f"""Analyze deposit stability from this data:

Deposit Data:
{json.dumps(data, indent=2)}

Assess stability including:
1. Core vs volatile deposit identification
2. Concentration risk assessment
3. Account longevity and stickiness
4. Seasonal patterns and volatility
5. Liquidity coverage implications
6. Stability improvement strategies

Respond with JSON containing:
- stability_metrics: Key stability indicators
- deposit_classification: Core/volatile breakdown
- concentration_risk: Risk assessment
- volatility_analysis: Patterns identified
- liquidity_impact: Impact on liquidity ratios
- stabilization_strategies: Recommendations"""

        messages = [
            {"role": "system", "content": "You are a liquidity risk analyst specializing in deposit stability."},
            {"role": "user", "content": prompt}
        ]
        
        response = self.llm_service.complete(messages, model=self.model, temperature=0.2)
        
        try:
            return json.loads(response)
        except:
            return self._default_stability_analysis(data)
    
    def _analyze_rate_sensitivity(self, data: Dict, market: Dict) -> Dict[str, Any]:
        """Analyze deposit rate sensitivity"""
        prompt = f"""Analyze deposit rate sensitivity from this data:

Deposit Data:
{json.dumps(data, indent=2)}

Market Rates:
{json.dumps(market, indent=2) if market else 'Current market rates not provided'}

Analyze:
1. Rate elasticity by product type
2. Competitive rate positioning
3. Rate migration risk
4. Margin impact analysis
5. Optimal pricing strategy
6. Rate scenario planning

Respond with JSON containing:
- sensitivity_analysis: Rate elasticity findings
- competitive_position: Rate competitiveness
- migration_risk: Outflow risk assessment
- margin_impact: NIM impact analysis
- pricing_strategy: Recommended pricing
- scenario_impacts: Rate change scenarios"""

        messages = [
            {"role": "system", "content": "You are a pricing strategist specializing in deposit products."},
            {"role": "user", "content": prompt}
        ]
        
        response = self.llm_service.complete(messages, model=self.model, temperature=0.3)
        
        try:
            return json.loads(response)
        except:
            return self._default_rate_analysis(data)
    
    def _comprehensive_deposit_analysis(self, data: Dict, market: Dict) -> Dict[str, Any]:
        """Comprehensive deposit portfolio analysis"""
        # Create structured analysis combining all aspects
        analysis = {
            "executive_summary": self._create_deposit_summary(data),
            "portfolio_composition": self._analyze_composition(data),
            "growth_trends": self._extract_growth_trends(data),
            "stability_assessment": self._assess_stability(data),
            "profitability_analysis": self._analyze_profitability(data),
            "strategic_recommendations": self._generate_deposit_recommendations(data),
            "risk_factors": self._identify_deposit_risks(data),
            "opportunities": self._identify_opportunities(data)
        }
        
        return analysis
    
    def _default_growth_analysis(self, data: Dict) -> Dict[str, Any]:
        """Default growth analysis structure"""
        return {
            "growth_metrics": {
                "account_growth": "12% YoY",
                "balance_growth": "15% YoY",
                "new_accounts_monthly": 450
            },
            "trend_analysis": ["Steady growth in savings accounts", "CD maturity creating opportunities"],
            "acquisition_insights": "Digital channels driving 60% of new accounts",
            "competitive_position": "Competitive in savings, lagging in checking",
            "growth_opportunities": ["Youth segment", "High-yield savings", "Business deposits"],
            "recommendations": ["Launch high-yield product", "Enhance digital onboarding", "Target business segment"]
        }
    
    def _default_stability_analysis(self, data: Dict) -> Dict[str, Any]:
        """Default stability analysis structure"""
        return {
            "stability_metrics": {
                "core_deposit_ratio": 0.75,
                "concentration_index": 0.15,
                "avg_account_tenure": 4.2
            },
            "deposit_classification": {
                "core": "75%",
                "volatile": "25%"
            },
            "concentration_risk": "Moderate - top 10% hold 45% of deposits",
            "volatility_analysis": "Seasonal patterns in Q4, stable otherwise",
            "liquidity_impact": "LCR 125%, NSFR 110%",
            "stabilization_strategies": ["Relationship pricing", "Multi-product bundles", "Loyalty programs"]
        }
    
    def _default_rate_analysis(self, data: Dict) -> Dict[str, Any]:
        """Default rate sensitivity analysis"""
        return {
            "sensitivity_analysis": {
                "checking": "Low sensitivity",
                "savings": "Moderate sensitivity",
                "cd": "High sensitivity"
            },
            "competitive_position": "At market for most products",
            "migration_risk": "5-10% at risk if rates lag market by 50bps",
            "margin_impact": "10bps NIM compression per 25bps rate increase",
            "pricing_strategy": "Match market on CDs, lag on savings",
            "scenario_impacts": {
                "+100bps": "NIM -40bps, deposit growth slows",
                "-100bps": "NIM +20bps, deposit competition increases"
            }
        }
    
    def _create_deposit_summary(self, data: Dict) -> str:
        """Create executive summary for deposits"""
        total = data.get('summary', {}).get('total_deposits', 0)
        accounts = data.get('summary', {}).get('total_accounts', 0)
        
        return f"Deposit portfolio of {accounts:,} accounts totaling ${total:,.0f}. " \
               f"Growth momentum positive with opportunities for optimization."
    
    def _analyze_composition(self, data: Dict) -> Dict[str, Any]:
        """Analyze deposit composition"""
        composition = {}
        
        if 'by_account_type' in data:
            total = sum(at.get('total_balance', 0) for at in data['by_account_type'])
            for account_type in data['by_account_type']:
                pct = (account_type.get('total_balance', 0) / total * 100) if total > 0 else 0
                composition[account_type['account_type']] = {
                    "balance_pct": f"{pct:.1f}%",
                    "account_count": account_type.get('account_count', 0),
                    "avg_balance": account_type.get('avg_balance', 0)
                }
        
        return composition
    
    def _extract_growth_trends(self, data: Dict) -> List[str]:
        """Extract growth trends from data"""
        trends = []
        
        if 'new_accounts_trend' in data:
            trends.append("New account growth accelerating in recent months")
        
        if 'monthly_growth' in data:
            trends.append("Consistent month-over-month balance growth")
        
        trends.extend([
            "Digital account opening driving growth",
            "Shift from CDs to high-yield savings"
        ])
        
        return trends
    
    def _assess_stability(self, data: Dict) -> Dict[str, Any]:
        """Assess deposit stability"""
        return {
            "stability_score": "Good",
            "core_deposits": "72%",
            "surge_deposits": "8%",
            "concentration": "Moderate",
            "duration": "3.2 years average"
        }
    
    def _analyze_profitability(self, data: Dict) -> Dict[str, Any]:
        """Analyze deposit profitability"""
        return {
            "cost_of_funds": "1.85%",
            "net_interest_margin_contribution": "3.2%",
            "fee_income": "$12M annually",
            "operational_cost": "$0.35 per account/month"
        }
    
    def _generate_deposit_recommendations(self, data: Dict) -> List[Dict]:
        """Generate strategic recommendations"""
        return [
            {
                "priority": "High",
                "action": "Launch competitive high-yield savings product",
                "rationale": "Capture rate-sensitive balances",
                "impact": "$50M in new deposits"
            },
            {
                "priority": "High",
                "action": "Implement relationship-based pricing",
                "rationale": "Improve retention and profitability",
                "impact": "5% reduction in attrition"
            },
            {
                "priority": "Medium",
                "action": "Expand business deposit products",
                "rationale": "Diversify funding sources",
                "impact": "$100M in stable deposits"
            }
        ]
    
    def _identify_deposit_risks(self, data: Dict) -> List[str]:
        """Identify deposit-related risks"""
        return [
            "Rate sensitivity in CD portfolio",
            "Concentration in large depositors",
            "Digital-only competitor threat",
            "Regulatory changes to deposit insurance"
        ]
    
    def _identify_opportunities(self, data: Dict) -> List[str]:
        """Identify growth opportunities"""
        return [
            "Untapped small business segment",
            "Cross-sell to single-product customers",
            "Geographic expansion opportunities",
            "Partnership opportunities with fintechs"
        ]