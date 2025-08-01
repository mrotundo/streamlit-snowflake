from typing import Dict, Any, Optional
from agents.tools.base_tool import BaseTool
from services.llm_interface import LLMInterface
from datetime import datetime, timedelta
import json


class AnalyzeDataLineageTool(BaseTool):
    """Tool for analyzing data lineage findings and providing specialized insights"""
    
    def __init__(self, llm_service: Optional[LLMInterface] = None, model: Optional[str] = None):
        super().__init__(
            name="AnalyzeDataLineage",
            description="Analyze data lineage information to identify issues and provide actionable insights"
        )
        self.llm_service = llm_service
        self.model = model or "gpt-4"
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "lineage_data": {
                "type": "dict",
                "description": "Data lineage information from TraceDataLineageTool"
            },
            "view_data": {
                "type": "dict",
                "description": "View data information from CheckViewDataTool",
                "optional": True
            },
            "job_status": {
                "type": "dict",
                "description": "Job status information from CheckJobStatusTool",
                "optional": True
            },
            "user_query": {
                "type": "string",
                "description": "The original user query about the data issue"
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Analyze data lineage and provide insights"""
        lineage_data = kwargs.get("lineage_data", {})
        view_data = kwargs.get("view_data", {})
        job_status = kwargs.get("job_status", {})
        user_query = kwargs.get("user_query", "")
        
        # Handle case where lineage_data might be the full tool output
        if isinstance(lineage_data, dict) and "lineage_chain" not in lineage_data and "result" in lineage_data:
            lineage_data = lineage_data.get("result", {})
        
        # Similarly for view_data and job_status
        if isinstance(view_data, dict) and "view_name" not in view_data and "result" in view_data:
            view_data = view_data.get("result", {})
            
        if isinstance(job_status, dict) and "job_runs" not in job_status and "result" in job_status:
            job_status = job_status.get("result", {})
        
        if not lineage_data or "lineage_chain" not in lineage_data:
            return {
                "success": False,
                "error": "lineage_data with lineage_chain is required",
                "result": {}
            }
        
        try:
            # Extract key information from lineage
            analysis = self._analyze_lineage_structure(lineage_data)
            
            # Analyze data freshness
            freshness_issues = self._analyze_data_freshness(lineage_data, view_data)
            
            # Analyze job failures and issues
            job_issues = self._analyze_job_issues(lineage_data, job_status)
            
            # Identify bottlenecks in the data pipeline
            bottlenecks = self._identify_bottlenecks(lineage_data, job_status)
            
            # Generate root cause analysis
            root_cause = self._determine_root_cause(
                analysis, freshness_issues, job_issues, bottlenecks, user_query
            )
            
            # Generate recommendations
            recommendations = self._generate_recommendations(
                root_cause, freshness_issues, job_issues, bottlenecks
            )
            
            # Create summary report
            summary = self._create_summary_report(
                analysis, freshness_issues, job_issues, bottlenecks, 
                root_cause, recommendations, user_query
            )
            
            return {
                "success": True,
                "result": {
                    "summary": summary,
                    "analysis": analysis,
                    "freshness_issues": freshness_issues,
                    "job_issues": job_issues,
                    "bottlenecks": bottlenecks,
                    "root_cause": root_cause,
                    "recommendations": recommendations,
                    "insights": {
                        "total_objects": analysis.get("total_objects", 0),
                        "critical_issues": len([i for i in freshness_issues + job_issues if i.get("severity") == "HIGH"]),
                        "data_lag_hours": self._calculate_max_data_lag(freshness_issues),
                        "failed_jobs": len([j for j in job_issues if j.get("type") == "Failed Job"])
                    }
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error analyzing data lineage: {str(e)}",
                "result": {}
            }
    
    def _analyze_lineage_structure(self, lineage_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze the structure of the data lineage"""
        lineage_chain = lineage_data.get("lineage_chain", [])
        
        # Count different object types
        views = [item for item in lineage_chain if item.get("object_type") == "view"]
        tables = [item for item in lineage_chain if item.get("object_type") == "table"]
        
        # Analyze view hierarchy
        view_levels = {}
        for view in views:
            level = view.get("level", 0)
            if level not in view_levels:
                view_levels[level] = []
            view_levels[level].append(view.get("object_name"))
        
        # Analyze table dependencies
        table_jobs = {}
        for table in tables:
            table_name = table.get("object_name")
            source_jobs = table.get("source_jobs", [])
            table_jobs[table_name] = {
                "job_count": len(source_jobs),
                "jobs": [job.get("job_name") for job in source_jobs],
                "last_loaded": table.get("last_loaded")
            }
        
        return {
            "total_objects": len(lineage_chain),
            "view_count": len(views),
            "table_count": len(tables),
            "view_hierarchy": view_levels,
            "table_dependencies": table_jobs,
            "max_view_depth": max(view_levels.keys()) if view_levels else 0
        }
    
    def _analyze_data_freshness(self, lineage_data: Dict[str, Any], view_data: Dict[str, Any]) -> list:
        """Analyze data freshness issues"""
        issues = []
        lineage_chain = lineage_data.get("lineage_chain", [])
        current_time = datetime.now()
        
        # Check table freshness
        for item in lineage_chain:
            if item.get("object_type") == "table" and item.get("last_loaded"):
                try:
                    last_loaded = datetime.fromisoformat(item["last_loaded"].replace('Z', '+00:00'))
                    age_hours = (current_time - last_loaded).total_seconds() / 3600
                    
                    if age_hours > 24:
                        severity = "HIGH" if age_hours > 48 else "MEDIUM"
                        issues.append({
                            "type": "Stale Table Data",
                            "severity": severity,
                            "object": item["object_name"],
                            "age_hours": round(age_hours, 1),
                            "last_update": item["last_loaded"],
                            "message": f"Table {item['object_name']} is {round(age_hours, 1)} hours old"
                        })
                except Exception:
                    pass
        
        # Check view data freshness if provided
        if view_data and "result" in view_data:
            view_info = view_data["result"]
            if "last_refreshed" in view_info and view_info["last_refreshed"]:
                try:
                    last_refreshed = datetime.fromisoformat(view_info["last_refreshed"].replace('Z', '+00:00'))
                    age_hours = (current_time - last_refreshed).total_seconds() / 3600
                    
                    if age_hours > 12:
                        issues.append({
                            "type": "Stale View",
                            "severity": "MEDIUM",
                            "object": view_info.get("view_name", "unknown"),
                            "age_hours": round(age_hours, 1),
                            "last_update": view_info["last_refreshed"],
                            "message": f"View hasn't been refreshed in {round(age_hours, 1)} hours"
                        })
                except Exception:
                    pass
        
        return issues
    
    def _analyze_job_issues(self, lineage_data: Dict[str, Any], job_status: Dict[str, Any]) -> list:
        """Analyze job execution issues"""
        issues = []
        
        # Check for failed jobs in lineage
        lineage_chain = lineage_data.get("lineage_chain", [])
        for item in lineage_chain:
            if item.get("object_type") == "table" and "source_jobs" in item:
                for job in item["source_jobs"]:
                    if job.get("status") == "FAILED":
                        issues.append({
                            "type": "Failed Job",
                            "severity": "HIGH",
                            "job_name": job["job_name"],
                            "table": item["object_name"],
                            "failed_at": job.get("start_time"),
                            "error": job.get("error_message", "Unknown error"),
                            "message": f"Job {job['job_name']} failed loading {item['object_name']}"
                        })
        
        # Analyze job status data if provided
        if job_status and "result" in job_status:
            stats = job_status["result"].get("statistics", {})
            
            # Check overall failure rate
            overall_stats = stats.get("overall_stats", {})
            if overall_stats.get("overall_success_rate", 100) < 90:
                issues.append({
                    "type": "High Failure Rate",
                    "severity": "MEDIUM",
                    "success_rate": overall_stats["overall_success_rate"],
                    "failed_runs": overall_stats.get("failed_runs", 0),
                    "total_runs": overall_stats.get("total_runs", 0),
                    "message": f"Overall job success rate is only {overall_stats['overall_success_rate']:.1f}%"
                })
            
            # Check for specific job issues
            job_issues_list = job_status["result"].get("issues", [])
            for issue in job_issues_list:
                if issue.get("type") == "Long Running Job":
                    issues.append({
                        "type": "Long Running Job",
                        "severity": issue.get("severity", "MEDIUM"),
                        "job_name": issue.get("job_name"),
                        "message": issue.get("message"),
                        "details": issue.get("details")
                    })
        
        return issues
    
    def _identify_bottlenecks(self, lineage_data: Dict[str, Any], job_status: Dict[str, Any]) -> list:
        """Identify bottlenecks in the data pipeline"""
        bottlenecks = []
        
        # Check for views with many dependencies
        lineage_chain = lineage_data.get("lineage_chain", [])
        for item in lineage_chain:
            if item.get("object_type") == "view":
                dep_count = len(item.get("dependencies", []))
                if dep_count > 5:
                    bottlenecks.append({
                        "type": "Complex View",
                        "severity": "MEDIUM",
                        "view_name": item["object_name"],
                        "dependency_count": dep_count,
                        "message": f"View {item['object_name']} has {dep_count} dependencies"
                    })
        
        # Check for tables with multiple loading jobs
        for item in lineage_chain:
            if item.get("object_type") == "table":
                job_count = len(item.get("source_jobs", []))
                if job_count > 3:
                    bottlenecks.append({
                        "type": "Multiple Load Jobs",
                        "severity": "LOW",
                        "table_name": item["object_name"],
                        "job_count": job_count,
                        "message": f"Table {item['object_name']} is loaded by {job_count} different jobs"
                    })
        
        return bottlenecks
    
    def _determine_root_cause(self, analysis: Dict, freshness_issues: list, 
                            job_issues: list, bottlenecks: list, user_query: str) -> Dict[str, Any]:
        """Determine the root cause of the data issue"""
        
        # Prioritize issues
        critical_issues = []
        
        # Failed jobs are usually the primary cause
        failed_jobs = [i for i in job_issues if i.get("type") == "Failed Job"]
        if failed_jobs:
            critical_issues.extend(failed_jobs)
        
        # Stale data is the next priority
        stale_tables = [i for i in freshness_issues if i.get("type") == "Stale Table Data" and i.get("severity") == "HIGH"]
        if stale_tables:
            critical_issues.extend(stale_tables)
        
        # Determine primary root cause
        if failed_jobs:
            primary_cause = "job_failure"
            description = f"Data loading jobs have failed, preventing fresh data from entering the system"
            affected_objects = list(set([j.get("table") for j in failed_jobs if j.get("table")]))
        elif stale_tables:
            primary_cause = "stale_data"
            description = f"Source tables haven't been updated in over 48 hours"
            affected_objects = [t.get("object") for t in stale_tables]
        elif freshness_issues:
            primary_cause = "data_delay"
            description = f"Data is experiencing delays in the refresh pipeline"
            affected_objects = [i.get("object") for i in freshness_issues]
        else:
            primary_cause = "unknown"
            description = "No specific data quality issues were identified"
            affected_objects = []
        
        return {
            "primary_cause": primary_cause,
            "description": description,
            "critical_issues": critical_issues,
            "affected_objects": affected_objects,
            "impact_level": "HIGH" if critical_issues else "MEDIUM"
        }
    
    def _generate_recommendations(self, root_cause: Dict, freshness_issues: list, 
                                job_issues: list, bottlenecks: list) -> list:
        """Generate actionable recommendations"""
        recommendations = []
        
        # Based on root cause
        if root_cause["primary_cause"] == "job_failure":
            recommendations.append({
                "priority": "HIGH",
                "action": "Investigate and fix failed jobs",
                "details": "Review error logs for failed jobs and address the root causes",
                "affected_jobs": list(set([j.get("job_name") for j in job_issues if j.get("type") == "Failed Job"]))
            })
            recommendations.append({
                "priority": "MEDIUM",
                "action": "Implement job monitoring alerts",
                "details": "Set up automated alerts for job failures to catch issues earlier"
            })
        
        elif root_cause["primary_cause"] == "stale_data":
            recommendations.append({
                "priority": "HIGH",
                "action": "Check job scheduler",
                "details": "Verify that scheduled jobs are running at expected intervals",
                "affected_tables": root_cause["affected_objects"]
            })
            recommendations.append({
                "priority": "MEDIUM",
                "action": "Review data source availability",
                "details": "Ensure upstream data sources are providing files on schedule"
            })
        
        # For high failure rates
        high_failure_rate = [i for i in job_issues if i.get("type") == "High Failure Rate"]
        if high_failure_rate:
            recommendations.append({
                "priority": "MEDIUM",
                "action": "Improve job reliability",
                "details": f"Job success rate is below 90%. Review common failure patterns and implement fixes"
            })
        
        # For complex views
        complex_views = [b for b in bottlenecks if b.get("type") == "Complex View"]
        if complex_views:
            recommendations.append({
                "priority": "LOW",
                "action": "Optimize complex views",
                "details": f"Consider simplifying views with many dependencies to improve performance",
                "views": [v.get("view_name") for v in complex_views]
            })
        
        # General recommendation for data freshness
        if freshness_issues:
            recommendations.append({
                "priority": "MEDIUM",
                "action": "Implement data freshness monitoring",
                "details": "Set up automated checks for data age and alert when thresholds are exceeded"
            })
        
        return recommendations
    
    def _create_summary_report(self, analysis: Dict, freshness_issues: list, job_issues: list,
                              bottlenecks: list, root_cause: Dict, recommendations: list,
                              user_query: str) -> str:
        """Create a comprehensive summary report"""
        
        # Use LLM to generate a natural language summary if available
        if self.llm_service:
            summary_data = {
                "user_query": user_query,
                "analysis": analysis,
                "root_cause": root_cause,
                "critical_issues": root_cause.get("critical_issues", []),
                "recommendations": recommendations,
                "data_flow": {
                    "views": analysis.get("view_count", 0),
                    "tables": analysis.get("table_count", 0),
                    "max_depth": analysis.get("max_view_depth", 0)
                }
            }
            
            prompt = f"""Based on this data lineage analysis, create a concise summary report:

User Query: {user_query}

Analysis Results:
{json.dumps(summary_data, indent=2)}

Create a summary that:
1. Directly addresses the user's concern
2. Explains the root cause in simple terms
3. Highlights the most critical issues
4. Provides clear next steps

Format as a brief executive summary (3-4 paragraphs max)."""

            messages = [
                {"role": "system", "content": "You are a data quality analyst providing clear, actionable insights."},
                {"role": "user", "content": prompt}
            ]
            
            try:
                summary = self.llm_service.complete(messages, model=self.model, temperature=0.3)
                return summary
            except Exception:
                pass
        
        # Fallback to template-based summary
        critical_count = len(root_cause.get("critical_issues", []))
        
        summary = f"""## Data Quality Analysis Summary

**Issue:** {user_query}

**Root Cause:** {root_cause['description']}

**Impact:** Found {critical_count} critical issues affecting {len(root_cause['affected_objects'])} data objects.

**Key Findings:**
- Total objects in data lineage: {analysis['total_objects']} ({analysis['view_count']} views, {analysis['table_count']} tables)
- Data freshness issues: {len(freshness_issues)} objects with stale data
- Job execution issues: {len(job_issues)} problems identified
- Pipeline bottlenecks: {len(bottlenecks)} potential performance impacts

**Recommended Actions:**
"""
        
        for i, rec in enumerate(recommendations[:3], 1):
            summary += f"\n{i}. **{rec['action']}** (Priority: {rec['priority']})\n   {rec['details']}"
        
        return summary
    
    def _calculate_max_data_lag(self, freshness_issues: list) -> float:
        """Calculate the maximum data lag in hours"""
        if not freshness_issues:
            return 0.0
        
        max_lag = 0.0
        for issue in freshness_issues:
            if "age_hours" in issue:
                max_lag = max(max_lag, issue["age_hours"])
        
        return max_lag