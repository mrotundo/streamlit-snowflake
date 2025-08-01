from typing import Dict, Any, Optional, List
from agents.tools.base_tool import BaseTool
from services.data_interface import DataInterface
from datetime import datetime, timedelta


class AnalyzeDataFreshnessTool(BaseTool):
    """Tool for analyzing data freshness and identifying staleness issues"""
    
    def __init__(self, data_service: Optional[DataInterface] = None):
        super().__init__(
            name="AnalyzeDataFreshness",
            description="Analyze when data was last updated and identify freshness issues"
        )
        self.data_service = data_service
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "object_name": {
                "type": "string",
                "description": "Name of the view or table to analyze"
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Analyze data freshness for a database object"""
        object_name = kwargs.get("object_name", "")
        
        if not object_name:
            return {
                "success": False,
                "error": "object_name is required",
                "result": {}
            }
        
        if not self.data_service or not self.data_service.validate_connection():
            return {
                "success": False,
                "error": "No data service connection available",
                "result": {}
            }
        
        try:
            # Determine object type
            object_type = self._determine_object_type(object_name)
            
            result = {
                "object_name": object_name,
                "object_type": object_type,
                "analysis_time": str(datetime.now())
            }
            
            if object_type == "view":
                # Analyze view freshness
                view_analysis = self._analyze_view_freshness(object_name)
                result.update(view_analysis)
            elif object_type == "table":
                # Analyze table freshness
                table_analysis = self._analyze_table_freshness(object_name)
                result.update(table_analysis)
            else:
                return {
                    "success": False,
                    "error": f"Object '{object_name}' not found in views or tables",
                    "result": {}
                }
            
            # Get data quality check results
            quality_checks = self._get_recent_quality_checks(object_name)
            result["recent_quality_checks"] = quality_checks
            
            # Generate freshness assessment
            assessment = self._generate_freshness_assessment(result)
            result["freshness_assessment"] = assessment
            
            return {
                "success": True,
                "result": result
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error analyzing freshness for {object_name}: {str(e)}",
                "result": {}
            }
    
    def _determine_object_type(self, object_name: str) -> Optional[str]:
        """Determine if object is a view or table"""
        # Check if it's a view
        view_query = "SELECT 1 FROM data_views WHERE LOWER(view_name) = LOWER(?)"
        if hasattr(self.data_service, 'connection_params'):
            view_query = view_query.replace('?', '%s')
        
        df = self.data_service.execute_query(view_query, {'object_name': object_name})
        if not df.empty:
            return "view"
        
        # Check if it's a table
        try:
            table_query = f"SELECT 1 FROM {object_name} LIMIT 1"
            df = self.data_service.execute_query(table_query)
            return "table"
        except:
            return None
    
    def _analyze_view_freshness(self, view_name: str) -> Dict[str, Any]:
        """Analyze freshness of a view by checking its dependencies"""
        analysis = {
            "view_dependencies": [],
            "stalest_dependency": None,
            "overall_data_age_hours": None
        }
        
        # Get view dependencies
        dep_query = """
            SELECT 
                vd.depends_on_object,
                vd.depends_on_type,
                dv.view_level
            FROM view_dependencies vd
            JOIN data_views dv ON vd.view_id = dv.view_id
            WHERE LOWER(dv.view_name) = LOWER(?)
        """
        
        if hasattr(self.data_service, 'connection_params'):
            dep_query = dep_query.replace('?', '%s')
        
        dep_df = self.data_service.execute_query(dep_query, {'view_name': view_name})
        
        max_age_hours = 0
        stalest_object = None
        
        for _, row in dep_df.iterrows():
            dep_name = row['depends_on_object']
            dep_type = row['depends_on_type']
            
            if dep_type == 'table':
                # Get table freshness
                table_freshness = self._get_table_last_update(dep_name)
                if table_freshness:
                    age_hours = (datetime.now() - table_freshness['last_update']).total_seconds() / 3600
                    
                    dep_info = {
                        "object_name": dep_name,
                        "object_type": dep_type,
                        "last_update": str(table_freshness['last_update']),
                        "age_hours": round(age_hours, 2),
                        "last_job": table_freshness.get('last_job')
                    }
                    
                    if age_hours > max_age_hours:
                        max_age_hours = age_hours
                        stalest_object = dep_info
                else:
                    dep_info = {
                        "object_name": dep_name,
                        "object_type": dep_type,
                        "last_update": None,
                        "age_hours": None,
                        "status": "No update history found"
                    }
                
                analysis["view_dependencies"].append(dep_info)
            elif dep_type == 'view':
                # Recursively check view freshness
                sub_view_freshness = self._analyze_view_freshness(dep_name)
                if sub_view_freshness.get("overall_data_age_hours"):
                    dep_info = {
                        "object_name": dep_name,
                        "object_type": dep_type,
                        "age_hours": sub_view_freshness["overall_data_age_hours"],
                        "indirect_dependencies": len(sub_view_freshness.get("view_dependencies", []))
                    }
                    
                    if sub_view_freshness["overall_data_age_hours"] > max_age_hours:
                        max_age_hours = sub_view_freshness["overall_data_age_hours"]
                        stalest_object = dep_info
                    
                    analysis["view_dependencies"].append(dep_info)
        
        if stalest_object:
            analysis["stalest_dependency"] = stalest_object
            analysis["overall_data_age_hours"] = round(max_age_hours, 2)
        
        return analysis
    
    def _analyze_table_freshness(self, table_name: str) -> Dict[str, Any]:
        """Analyze freshness of a table"""
        analysis = {
            "last_update_info": None,
            "update_history": [],
            "update_frequency": None
        }
        
        # Get last update information
        last_update = self._get_table_last_update(table_name)
        if last_update:
            age_hours = (datetime.now() - last_update['last_update']).total_seconds() / 3600
            analysis["last_update_info"] = {
                "timestamp": str(last_update['last_update']),
                "age_hours": round(age_hours, 2),
                "job_name": last_update.get('job_name'),
                "rows_affected": last_update.get('rows_inserted', 0)
            }
        
        # Get update history
        history_query = """
            SELECT 
                jr.start_time,
                jr.end_time,
                jr.status,
                j.job_name,
                jrt.rows_inserted,
                jrt.rows_updated
            FROM job_run_target_tables jrt
            JOIN job_runs jr ON jrt.job_run_id = jr.job_run_id
            JOIN jobs j ON jr.job_id = j.job_id
            WHERE LOWER(jrt.table_name) = LOWER(?)
            ORDER BY jr.start_time DESC
            LIMIT 20
        """
        
        if hasattr(self.data_service, 'connection_params'):
            history_query = history_query.replace('?', '%s')
        
        history_df = self.data_service.execute_query(history_query, {'table_name': table_name})
        
        update_times = []
        for _, row in history_df.iterrows():
            update_info = {
                "timestamp": str(row['start_time']),
                "status": row['status'],
                "job_name": row['job_name'],
                "rows_affected": (row['rows_inserted'] or 0) + (row['rows_updated'] or 0)
            }
            analysis["update_history"].append(update_info)
            
            if row['status'] == 'SUCCESS':
                update_times.append(datetime.fromisoformat(str(row['start_time']).replace('Z', '+00:00')))
        
        # Calculate update frequency
        if len(update_times) >= 2:
            # Calculate average time between updates
            time_gaps = []
            for i in range(1, len(update_times)):
                gap = (update_times[i-1] - update_times[i]).total_seconds() / 3600
                time_gaps.append(gap)
            
            avg_gap = sum(time_gaps) / len(time_gaps)
            
            analysis["update_frequency"] = {
                "average_hours_between_updates": round(avg_gap, 2),
                "update_pattern": self._determine_update_pattern(avg_gap),
                "last_5_gaps_hours": [round(gap, 2) for gap in time_gaps[:5]]
            }
        
        return analysis
    
    def _get_table_last_update(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get the last successful update for a table"""
        query = """
            SELECT 
                jr.end_time as last_update,
                j.job_name,
                jrt.rows_inserted
            FROM job_run_target_tables jrt
            JOIN job_runs jr ON jrt.job_run_id = jr.job_run_id
            JOIN jobs j ON jr.job_id = j.job_id
            WHERE LOWER(jrt.table_name) = LOWER(?)
            AND jr.status = 'SUCCESS'
            ORDER BY jr.end_time DESC
            LIMIT 1
        """
        
        if hasattr(self.data_service, 'connection_params'):
            query = query.replace('?', '%s')
        
        df = self.data_service.execute_query(query, {'table_name': table_name})
        
        if not df.empty:
            row = df.iloc[0]
            return {
                "last_update": datetime.fromisoformat(str(row['last_update']).replace('Z', '+00:00')),
                "job_name": row['job_name'],
                "rows_inserted": row['rows_inserted']
            }
        return None
    
    def _get_recent_quality_checks(self, object_name: str) -> List[Dict[str, Any]]:
        """Get recent data quality check results"""
        query = """
            SELECT 
                check_type,
                check_result,
                check_value,
                threshold_value,
                status,
                check_timestamp
            FROM data_quality_checks
            WHERE LOWER(object_name) = LOWER(?)
            ORDER BY check_timestamp DESC
            LIMIT 5
        """
        
        if hasattr(self.data_service, 'connection_params'):
            query = query.replace('?', '%s')
        
        df = self.data_service.execute_query(query, {'object_name': object_name})
        
        checks = []
        for _, row in df.iterrows():
            checks.append({
                "type": row['check_type'],
                "result": row['check_result'],
                "value": row['check_value'],
                "threshold": row['threshold_value'],
                "status": row['status'],
                "timestamp": str(row['check_timestamp'])
            })
        
        return checks
    
    def _determine_update_pattern(self, avg_hours: float) -> str:
        """Determine the update pattern based on average hours between updates"""
        if avg_hours < 0.5:
            return "Real-time (< 30 minutes)"
        elif avg_hours < 1.5:
            return "Hourly"
        elif avg_hours < 6:
            return "Multiple times daily"
        elif avg_hours < 30:
            return "Daily"
        elif avg_hours < 200:
            return "Weekly"
        else:
            return "Irregular"
    
    def _generate_freshness_assessment(self, analysis_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate an overall freshness assessment"""
        assessment = {
            "status": "UNKNOWN",
            "summary": "",
            "recommendations": []
        }
        
        if analysis_data["object_type"] == "view":
            if analysis_data.get("overall_data_age_hours") is not None:
                age = analysis_data["overall_data_age_hours"]
                
                if age < 2:
                    assessment["status"] = "FRESH"
                    assessment["summary"] = f"View data is fresh (last updated {age:.1f} hours ago)"
                elif age < 24:
                    assessment["status"] = "ACCEPTABLE"
                    assessment["summary"] = f"View data is reasonably fresh (last updated {age:.1f} hours ago)"
                elif age < 48:
                    assessment["status"] = "STALE"
                    assessment["summary"] = f"View data is becoming stale (last updated {age:.1f} hours ago)"
                    assessment["recommendations"].append("Check if source data loads are running as expected")
                else:
                    assessment["status"] = "VERY_STALE"
                    assessment["summary"] = f"View data is very stale (last updated {age:.1f} hours ago)"
                    assessment["recommendations"].append("Investigate why source data hasn't been updated")
                    assessment["recommendations"].append("Check job execution logs for failures")
                
                # Check stalest dependency
                if analysis_data.get("stalest_dependency"):
                    dep = analysis_data["stalest_dependency"]
                    assessment["recommendations"].append(
                        f"Focus on {dep['object_name']} which hasn't been updated in {dep['age_hours']:.1f} hours"
                    )
        
        elif analysis_data["object_type"] == "table":
            if analysis_data.get("last_update_info"):
                age = analysis_data["last_update_info"]["age_hours"]
                
                # Consider update frequency
                if analysis_data.get("update_frequency"):
                    expected_gap = analysis_data["update_frequency"]["average_hours_between_updates"]
                    
                    if age > expected_gap * 2:
                        assessment["status"] = "OVERDUE"
                        assessment["summary"] = f"Table update is overdue (last updated {age:.1f} hours ago, expected every {expected_gap:.1f} hours)"
                        assessment["recommendations"].append("Check if the loading job has failed or been disabled")
                    elif age > expected_gap * 1.5:
                        assessment["status"] = "DELAYED"
                        assessment["summary"] = f"Table update is delayed (last updated {age:.1f} hours ago, expected every {expected_gap:.1f} hours)"
                    else:
                        assessment["status"] = "ON_SCHEDULE"
                        assessment["summary"] = f"Table is updating on schedule (last updated {age:.1f} hours ago)"
                else:
                    # No frequency data, use absolute thresholds
                    if age < 24:
                        assessment["status"] = "CURRENT"
                        assessment["summary"] = f"Table data is current (last updated {age:.1f} hours ago)"
                    else:
                        assessment["status"] = "POTENTIALLY_STALE"
                        assessment["summary"] = f"Table may be stale (last updated {age:.1f} hours ago)"
        
        return assessment