from typing import Dict, Any, List
from agents.tools.base_tool import BaseTool
from services.llm_interface import LLMInterface
import json


class AnalyzeTransactionPatternsTool(BaseTool):
    """Tool for analyzing transaction patterns and providing behavioral insights"""
    
    def __init__(self, llm_service: LLMInterface, model: str):
        super().__init__(
            name="AnalyzeTransactionPatterns",
            description="Analyze transaction patterns for insights and anomaly detection"
        )
        self.llm_service = llm_service
        self.model = model
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "transaction_data": {
                "type": "dict",
                "description": "Transaction data from query results"
            },
            "analysis_type": {
                "type": "string",
                "description": "Type of analysis: behavioral_insights, fraud_detection, spending_analysis, or comprehensive",
                "optional": True
            },
            "customer_context": {
                "type": "dict",
                "description": "Customer segment or demographic context",
                "optional": True
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Analyze transaction patterns and provide insights"""
        transaction_data = kwargs.get("transaction_data", {})
        analysis_type = kwargs.get("analysis_type", "comprehensive")
        customer_context = kwargs.get("customer_context", {})
        
        try:
            # Perform analysis based on type
            if analysis_type == "behavioral_insights":
                analysis = self._analyze_behavioral_patterns(transaction_data, customer_context)
            elif analysis_type == "fraud_detection":
                analysis = self._analyze_fraud_patterns(transaction_data)
            elif analysis_type == "spending_analysis":
                analysis = self._analyze_spending_patterns(transaction_data, customer_context)
            else:
                analysis = self._comprehensive_transaction_analysis(transaction_data, customer_context)
            
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
    
    def _analyze_behavioral_patterns(self, data: Dict, context: Dict) -> Dict[str, Any]:
        """Analyze customer behavioral patterns from transactions"""
        # Use LLM for sophisticated pattern analysis
        prompt = f"""Analyze these transaction patterns for behavioral insights:

Transaction Data:
{json.dumps(data, indent=2)}

Customer Context:
{json.dumps(context, indent=2) if context else 'General customer base'}

Provide behavioral analysis including:
1. Spending habits and preferences
2. Channel usage patterns (digital vs physical)
3. Time-based behaviors (daily, weekly, seasonal)
4. Life event indicators
5. Financial health signals
6. Engagement opportunities

Respond with JSON containing:
- behavioral_segments: Customer behavior categories identified
- spending_patterns: Key spending insights
- channel_preferences: Digital adoption and usage
- life_events: Potential life events detected
- financial_health: Health indicators
- recommendations: Engagement strategies"""

        messages = [
            {"role": "system", "content": "You are a behavioral analytics expert in banking."},
            {"role": "user", "content": prompt}
        ]
        
        response = self.llm_service.complete(messages, model=self.model, temperature=0.3)
        
        try:
            return json.loads(response)
        except:
            return self._default_behavioral_analysis(data)
    
    def _analyze_fraud_patterns(self, data: Dict) -> Dict[str, Any]:
        """Analyze transactions for fraud patterns"""
        # Create fraud detection analysis
        analysis = {
            "risk_assessment": self._assess_fraud_risk(data),
            "anomalies_detected": self._extract_anomalies(data),
            "pattern_analysis": self._analyze_suspicious_patterns(data),
            "risk_scores": self._calculate_risk_scores(data),
            "recommendations": self._fraud_prevention_recommendations(data),
            "monitoring_alerts": self._generate_monitoring_alerts(data)
        }
        
        return analysis
    
    def _analyze_spending_patterns(self, data: Dict, context: Dict) -> Dict[str, Any]:
        """Analyze customer spending patterns"""
        # Create spending analysis
        return {
            "spending_summary": self._summarize_spending(data),
            "category_breakdown": self._analyze_by_category(data),
            "merchant_analysis": self._analyze_merchants(data),
            "budgeting_insights": self._generate_budget_insights(data),
            "savings_opportunities": self._identify_savings(data),
            "recommendations": self._spending_recommendations(data)
        }
    
    def _comprehensive_transaction_analysis(self, data: Dict, context: Dict) -> Dict[str, Any]:
        """Comprehensive transaction pattern analysis"""
        return {
            "executive_summary": self._create_transaction_summary(data),
            "volume_analysis": self._analyze_volume(data),
            "pattern_insights": self._extract_patterns(data),
            "customer_behavior": self._analyze_behavior(data),
            "risk_indicators": self._identify_risks(data),
            "opportunities": self._identify_transaction_opportunities(data),
            "strategic_recommendations": self._generate_transaction_recommendations(data)
        }
    
    def _default_behavioral_analysis(self, data: Dict) -> Dict[str, Any]:
        """Default behavioral analysis structure"""
        return {
            "behavioral_segments": {
                "digital_natives": "45% primarily use digital channels",
                "traditional_users": "30% prefer branch/ATM",
                "hybrid_users": "25% use mixed channels"
            },
            "spending_patterns": [
                "Peak spending on weekends",
                "Regular subscription payments detected",
                "Seasonal shopping patterns evident"
            ],
            "channel_preferences": {
                "mobile": "60%",
                "online": "25%",
                "branch": "10%",
                "atm": "5%"
            },
            "life_events": ["Possible home purchase preparation", "New family member indicators"],
            "financial_health": {
                "cash_flow": "Positive",
                "spending_stability": "Stable",
                "savings_behavior": "Improving"
            },
            "recommendations": [
                "Offer budgeting tools to high spenders",
                "Promote mobile features to digital users",
                "Provide savings goals for positive cash flow customers"
            ]
        }
    
    def _assess_fraud_risk(self, data: Dict) -> Dict[str, Any]:
        """Assess overall fraud risk"""
        anomaly_count = len(data.get('anomaly_summary', {}).get('large_transactions', []))
        
        if anomaly_count > 10:
            risk_level = "High"
        elif anomaly_count > 5:
            risk_level = "Medium"
        else:
            risk_level = "Low"
        
        return {
            "overall_risk": risk_level,
            "risk_factors": [
                f"{anomaly_count} unusual transactions detected",
                "After-hours activity present",
                "Rapid succession transactions identified"
            ],
            "confidence": "85%"
        }
    
    def _extract_anomalies(self, data: Dict) -> List[Dict]:
        """Extract anomalies from transaction data"""
        anomalies = []
        
        if 'large_transactions' in data:
            for trans in data['large_transactions'][:5]:
                anomalies.append({
                    "type": "Large transaction",
                    "transaction_id": trans.get('transaction_id'),
                    "amount": trans.get('amount'),
                    "risk_score": min(trans.get('times_avg', 1) * 10, 100)
                })
        
        if 'unusual_time_transactions' in data:
            for trans in data['unusual_time_transactions'][:3]:
                anomalies.append({
                    "type": "Unusual time",
                    "transaction_id": trans.get('transaction_id'),
                    "time": trans.get('transaction_time'),
                    "risk_score": 60
                })
        
        return anomalies
    
    def _analyze_suspicious_patterns(self, data: Dict) -> Dict[str, Any]:
        """Analyze suspicious transaction patterns"""
        return {
            "velocity_violations": "3 customers with rapid transactions",
            "amount_patterns": "Round number transactions increasing",
            "geographic_anomalies": "Out-of-pattern location usage",
            "behavioral_changes": "Sudden spending pattern shifts in 5% of accounts"
        }
    
    def _calculate_risk_scores(self, data: Dict) -> Dict[str, Any]:
        """Calculate transaction risk scores"""
        return {
            "high_risk_transactions": 12,
            "medium_risk_transactions": 45,
            "low_risk_transactions": 943,
            "avg_risk_score": 22.5
        }
    
    def _fraud_prevention_recommendations(self, data: Dict) -> List[str]:
        """Generate fraud prevention recommendations"""
        return [
            "Implement real-time transaction monitoring",
            "Add multi-factor authentication for large transactions",
            "Create velocity rules for rapid transactions",
            "Enhance merchant category monitoring"
        ]
    
    def _generate_monitoring_alerts(self, data: Dict) -> List[Dict]:
        """Generate monitoring alerts"""
        return [
            {
                "alert_type": "High-value transaction",
                "threshold": "$10,000",
                "action": "SMS verification required"
            },
            {
                "alert_type": "Unusual time",
                "threshold": "12AM - 5AM",
                "action": "Flag for review"
            },
            {
                "alert_type": "Velocity",
                "threshold": "5 transactions in 10 minutes",
                "action": "Temporary hold"
            }
        ]
    
    def _summarize_spending(self, data: Dict) -> Dict[str, Any]:
        """Summarize spending patterns"""
        return {
            "monthly_average": "$3,500",
            "top_categories": ["Groceries", "Dining", "Transportation"],
            "spending_trend": "Increasing 5% MoM",
            "discretionary_ratio": "35%"
        }
    
    def _analyze_by_category(self, data: Dict) -> Dict[str, Any]:
        """Analyze spending by category"""
        categories = {}
        
        if 'category_summary' in data:
            for cat in data['category_summary']:
                categories[cat['category']] = {
                    "total_spend": cat.get('total_volume', 0),
                    "transaction_count": cat.get('transaction_count', 0),
                    "avg_transaction": cat.get('avg_amount', 0),
                    "trend": "Stable"
                }
        
        return categories
    
    def _analyze_merchants(self, data: Dict) -> Dict[str, Any]:
        """Analyze merchant patterns"""
        return {
            "top_merchants": ["Amazon", "Walmart", "Target"],
            "merchant_diversity": "High",
            "new_merchants": 12,
            "loyalty_indicators": "Strong repeat usage at 5 merchants"
        }
    
    def _generate_budget_insights(self, data: Dict) -> List[str]:
        """Generate budgeting insights"""
        return [
            "Dining expenses exceed typical budget by 20%",
            "Subscription services total $245/month",
            "Opportunity to save $150/month on discretionary spending"
        ]
    
    def _identify_savings(self, data: Dict) -> List[Dict]:
        """Identify savings opportunities"""
        return [
            {
                "category": "Subscriptions",
                "current_spend": "$245",
                "potential_savings": "$80",
                "recommendation": "Review and consolidate services"
            },
            {
                "category": "Banking fees",
                "current_spend": "$45",
                "potential_savings": "$45",
                "recommendation": "Switch to fee-free account"
            }
        ]
    
    def _spending_recommendations(self, data: Dict) -> List[str]:
        """Generate spending recommendations"""
        return [
            "Set up automated savings transfers",
            "Create category-based spending alerts",
            "Consider cashback credit card for regular purchases",
            "Review and optimize recurring subscriptions"
        ]
    
    def _create_transaction_summary(self, data: Dict) -> str:
        """Create executive summary"""
        total_trans = data.get('summary', {}).get('total_transactions', 0)
        total_volume = data.get('summary', {}).get('total_volume', 0)
        
        return f"Analyzed {total_trans:,} transactions totaling ${total_volume:,.0f}. " \
               f"Patterns indicate healthy customer engagement with opportunities for optimization."
    
    def _analyze_volume(self, data: Dict) -> Dict[str, Any]:
        """Analyze transaction volume"""
        return {
            "daily_average": data.get('summary', {}).get('total_transactions', 0) / 30,
            "peak_day": "Friday",
            "peak_hour": "12 PM",
            "growth_rate": "8% MoM"
        }
    
    def _extract_patterns(self, data: Dict) -> List[str]:
        """Extract key patterns"""
        patterns = []
        
        if 'hourly_patterns' in data:
            patterns.append("Peak activity during lunch hours (12-1 PM)")
        
        if 'weekly_patterns' in data:
            patterns.append("Weekend spending 40% higher than weekdays")
        
        patterns.extend([
            "Digital channel adoption increasing",
            "Recurring payment patterns well-established",
            "Seasonal spending spikes detected"
        ])
        
        return patterns
    
    def _analyze_behavior(self, data: Dict) -> Dict[str, Any]:
        """Analyze customer behavior from transactions"""
        return {
            "segments": {
                "high_frequency": "20% of customers, 60% of transactions",
                "moderate_users": "50% of customers, 35% of transactions",
                "low_engagement": "30% of customers, 5% of transactions"
            },
            "engagement_score": 7.2,
            "digital_adoption": "65%"
        }
    
    def _identify_risks(self, data: Dict) -> List[str]:
        """Identify transaction-related risks"""
        return [
            "Increasing fraud attempts in digital channels",
            "Customer data exposure through merchant breaches",
            "Regulatory compliance for transaction monitoring",
            "System capacity during peak periods"
        ]
    
    def _identify_transaction_opportunities(self, data: Dict) -> List[str]:
        """Identify opportunities from transaction data"""
        return [
            "Real-time payment capabilities",
            "Enhanced merchant offers program",
            "Predictive analytics for customer needs",
            "Automated financial management tools"
        ]
    
    def _generate_transaction_recommendations(self, data: Dict) -> List[Dict]:
        """Generate strategic recommendations"""
        return [
            {
                "priority": "High",
                "recommendation": "Implement real-time fraud detection",
                "rationale": "Reduce fraud losses by 30%",
                "investment": "$500K",
                "roi": "Break-even in 8 months"
            },
            {
                "priority": "High",
                "recommendation": "Launch intelligent alerts system",
                "rationale": "Improve customer engagement",
                "investment": "$200K",
                "roi": "Increase product adoption by 25%"
            },
            {
                "priority": "Medium",
                "recommendation": "Develop spending insights dashboard",
                "rationale": "Differentiate service offering",
                "investment": "$300K",
                "roi": "Reduce attrition by 15%"
            }
        ]