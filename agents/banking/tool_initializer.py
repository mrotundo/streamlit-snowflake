from typing import Dict, Any
from agents.tools.base_tool import BaseTool
from services.llm_interface import LLMInterface
from services.data_interface import DataInterface

# Import all the new tools
from agents.tools.banking.customer_query_tool import CustomerQueryTool
from agents.tools.banking.loan_query_tool import LoanQueryTool
from agents.tools.banking.deposit_query_tool import DepositQueryTool
from agents.tools.banking.transaction_query_tool import TransactionQueryTool
from agents.tools.banking.analyze_customer_segments_tool import AnalyzeCustomerSegmentsTool
from agents.tools.banking.analyze_loan_portfolio_tool import AnalyzeLoanPortfolioTool
from agents.tools.banking.analyze_deposit_trends_tool import AnalyzeDepositTrendsTool
from agents.tools.banking.analyze_transaction_patterns_tool import AnalyzeTransactionPatternsTool


def initialize_banking_tools(llm_service: LLMInterface, model: str, data_service: DataInterface) -> Dict[str, BaseTool]:
    """Initialize all banking tools with their dependencies"""
    tools = {}
    
    # SQL Query Tools
    tools["CustomerQuery"] = CustomerQueryTool(data_service)
    tools["LoanQuery"] = LoanQueryTool(data_service)
    tools["DepositQuery"] = DepositQueryTool(data_service)
    tools["TransactionQuery"] = TransactionQueryTool(data_service)
    
    # Analysis Tools
    tools["AnalyzeCustomerSegments"] = AnalyzeCustomerSegmentsTool(llm_service, model)
    tools["AnalyzeLoanPortfolio"] = AnalyzeLoanPortfolioTool(llm_service, model)
    tools["AnalyzeDepositTrends"] = AnalyzeDepositTrendsTool(llm_service, model)
    tools["AnalyzeTransactionPatterns"] = AnalyzeTransactionPatternsTool(llm_service, model)
    
    return tools