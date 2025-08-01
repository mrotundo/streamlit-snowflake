from typing import Dict, Any, Optional, List
from agents.tools.base_tool import BaseTool
from services.data_interface import DataInterface
from datetime import datetime, timedelta


class CheckJobStatusTool(BaseTool):
    """Tool for checking job execution status and history"""
    
    def __init__(self, data_service: Optional[DataInterface] = None):
        super().__init__(
            name="CheckJobStatus",
            description="Check the execution status and history of data loading jobs"
        )
        self.data_service = data_service
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "job_name": {
                "type": "string",
                "description": "Specific job name to check (optional, checks all if not provided)",
                "optional": True
            },
            "time_range": {
                "type": "string",
                "description": "Time range to check: 'last 24 hours', 'last 48 hours', 'last week', etc.",
                "optional": True
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Check job execution status and history"""
        job_name = kwargs.get("job_name", None)
        time_range = kwargs.get("time_range", "last 24 hours")
        
        if not self.data_service or not self.data_service.validate_connection():
            return {
                "success": False,
                "error": "No data service connection available",
                "result": {}
            }
        
        try:
            # Parse time range
            start_time = self._parse_time_range(time_range)
            
            # Build query
            if job_name:
                query = """
                    SELECT 
                        jr.job_run_id,
                        j.job_id,
                        j.job_name,
                        j.job_type,
                        jr.start_time,
                        jr.end_time,
                        jr.status,
                        jr.rows_processed,
                        jr.error_message,
                        CASE 
                            WHEN jr.end_time IS NOT NULL 
                            THEN (JULIANDAY(jr.end_time) - JULIANDAY(jr.start_time)) * 24 * 60
                            ELSE NULL
                        END as duration_minutes
                    FROM job_runs jr
                    JOIN jobs j ON jr.job_id = j.job_id
                    WHERE j.job_name = ?
                    AND jr.start_time >= ?
                    ORDER BY jr.start_time DESC
                """
                params = {'job_name': job_name, 'start_time': start_time}
            else:
                query = """
                    SELECT 
                        jr.job_run_id,
                        j.job_id,
                        j.job_name,
                        j.job_type,
                        jr.start_time,
                        jr.end_time,
                        jr.status,
                        jr.rows_processed,
                        jr.error_message,
                        CASE 
                            WHEN jr.end_time IS NOT NULL 
                            THEN (JULIANDAY(jr.end_time) - JULIANDAY(jr.start_time)) * 24 * 60
                            ELSE NULL
                        END as duration_minutes
                    FROM job_runs jr
                    JOIN jobs j ON jr.job_id = j.job_id
                    WHERE jr.start_time >= ?
                    ORDER BY jr.start_time DESC
                    LIMIT 100
                """
                params = {'start_time': start_time}
            
            # Adjust for Snowflake
            if hasattr(self.data_service, 'connection_params'):
                query = query.replace('?', '%s')
                query = query.replace('JULIANDAY(jr.end_time) - JULIANDAY(jr.start_time)',
                                    'DATEDIFF(minute, jr.start_time, jr.end_time)')
            
            df = self.data_service.execute_query(query, params)
            
            # Process results
            job_runs = []
            for _, row in df.iterrows():
                job_runs.append({
                    "job_run_id": row['job_run_id'],
                    "job_id": row['job_id'],
                    "job_name": row['job_name'],
                    "job_type": row['job_type'],
                    "start_time": str(row['start_time']),
                    "end_time": str(row['end_time']) if row['end_time'] else None,
                    "status": row['status'],
                    "rows_processed": int(row['rows_processed']) if row['rows_processed'] else 0,
                    "duration_minutes": float(row['duration_minutes']) if row['duration_minutes'] else None,
                    "error_message": row['error_message']
                })
            
            # Calculate statistics
            stats = self._calculate_job_statistics(job_runs)
            
            # Identify issues
            issues = self._identify_job_issues(job_runs)
            
            # Get job dependencies if checking specific job
            dependencies = {}
            if job_name:
                dependencies = self._get_job_dependencies(job_name)
            
            result = {
                "time_range": {
                    "start": str(start_time),
                    "end": str(datetime.now()),
                    "description": time_range
                },
                "total_runs": len(job_runs),
                "job_runs": job_runs,
                "statistics": stats,
                "issues": issues
            }
            
            if dependencies:
                result["dependencies"] = dependencies
            
            return {
                "success": True,
                "result": result
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error checking job status: {str(e)}",
                "result": {}
            }
    
    def _parse_time_range(self, time_range: str) -> datetime:
        """Parse time range string to datetime"""
        now = datetime.now()
        time_range_lower = time_range.lower()
        
        if "hour" in time_range_lower:
            # Extract number of hours
            import re
            match = re.search(r'(\d+)', time_range_lower)
            hours = int(match.group(1)) if match else 24
            return now - timedelta(hours=hours)
        elif "day" in time_range_lower:
            match = re.search(r'(\d+)', time_range_lower)
            days = int(match.group(1)) if match else 1
            return now - timedelta(days=days)
        elif "week" in time_range_lower:
            match = re.search(r'(\d+)', time_range_lower)
            weeks = int(match.group(1)) if match else 1
            return now - timedelta(weeks=weeks)
        else:
            # Default to last 24 hours
            return now - timedelta(hours=24)
    
    def _calculate_job_statistics(self, job_runs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate statistics from job runs"""
        if not job_runs:
            return {}
        
        # Group by job name
        job_stats = {}
        for run in job_runs:
            job_name = run['job_name']
            if job_name not in job_stats:
                job_stats[job_name] = {
                    "total_runs": 0,
                    "successful_runs": 0,
                    "failed_runs": 0,
                    "in_progress_runs": 0,
                    "success_rate": 0.0,
                    "avg_duration_minutes": 0.0,
                    "total_rows_processed": 0,
                    "last_run_status": None,
                    "last_run_time": None
                }
            
            stats = job_stats[job_name]
            stats["total_runs"] += 1
            
            if run['status'] == 'SUCCESS':
                stats["successful_runs"] += 1
            elif run['status'] == 'FAILED':
                stats["failed_runs"] += 1
            elif run['status'] == 'IN_PROGRESS':
                stats["in_progress_runs"] += 1
            
            if run['rows_processed']:
                stats["total_rows_processed"] += run['rows_processed']
            
            # Track last run
            if not stats["last_run_time"] or run['start_time'] > stats["last_run_time"]:
                stats["last_run_time"] = run['start_time']
                stats["last_run_status"] = run['status']
        
        # Calculate averages
        for job_name, stats in job_stats.items():
            if stats["total_runs"] > 0:
                stats["success_rate"] = (stats["successful_runs"] / stats["total_runs"]) * 100
                
                # Calculate average duration for successful runs
                successful_durations = [
                    run['duration_minutes'] 
                    for run in job_runs 
                    if run['job_name'] == job_name 
                    and run['status'] == 'SUCCESS' 
                    and run['duration_minutes'] is not None
                ]
                if successful_durations:
                    stats["avg_duration_minutes"] = sum(successful_durations) / len(successful_durations)
        
        # Overall statistics
        overall_stats = {
            "total_jobs": len(job_stats),
            "total_runs": len(job_runs),
            "successful_runs": sum(1 for run in job_runs if run['status'] == 'SUCCESS'),
            "failed_runs": sum(1 for run in job_runs if run['status'] == 'FAILED'),
            "in_progress_runs": sum(1 for run in job_runs if run['status'] == 'IN_PROGRESS'),
            "overall_success_rate": 0.0,
            "by_job": job_stats
        }
        
        if overall_stats["total_runs"] > 0:
            overall_stats["overall_success_rate"] = (overall_stats["successful_runs"] / overall_stats["total_runs"]) * 100
        
        return overall_stats
    
    def _identify_job_issues(self, job_runs: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """Identify issues with job executions"""
        issues = []
        
        # Check for recent failures
        recent_failures = [run for run in job_runs if run['status'] == 'FAILED']
        if recent_failures:
            for failure in recent_failures[:5]:  # Show up to 5 recent failures
                issues.append({
                    "severity": "HIGH",
                    "type": "Job Failure",
                    "job_name": failure['job_name'],
                    "message": f"Job '{failure['job_name']}' failed at {failure['start_time']}",
                    "details": failure.get('error_message', 'No error message available'),
                    "job_run_id": failure['job_run_id']
                })
        
        # Check for jobs stuck in progress
        in_progress = [run for run in job_runs if run['status'] == 'IN_PROGRESS']
        for run in in_progress:
            start_time = datetime.fromisoformat(run['start_time'].replace('Z', '+00:00'))
            duration_hours = (datetime.now() - start_time).total_seconds() / 3600
            
            if duration_hours > 2:  # Job running for more than 2 hours
                issues.append({
                    "severity": "MEDIUM",
                    "type": "Long Running Job",
                    "job_name": run['job_name'],
                    "message": f"Job '{run['job_name']}' has been running for {duration_hours:.1f} hours",
                    "details": f"Started at {run['start_time']}",
                    "job_run_id": run['job_run_id']
                })
        
        # Check for missing scheduled runs
        job_groups = {}
        for run in job_runs:
            if run['job_name'] not in job_groups:
                job_groups[run['job_name']] = []
            job_groups[run['job_name']].append(run)
        
        for job_name, runs in job_groups.items():
            if len(runs) >= 2:
                # Sort by start time
                sorted_runs = sorted(runs, key=lambda x: x['start_time'])
                
                # Check gaps between runs
                for i in range(1, len(sorted_runs)):
                    prev_start = datetime.fromisoformat(sorted_runs[i-1]['start_time'].replace('Z', '+00:00'))
                    curr_start = datetime.fromisoformat(sorted_runs[i]['start_time'].replace('Z', '+00:00'))
                    gap_hours = (curr_start - prev_start).total_seconds() / 3600
                    
                    # If gap is more than 25 hours for a daily job
                    if gap_hours > 25 and 'daily' in job_name.lower():
                        issues.append({
                            "severity": "LOW",
                            "type": "Missed Schedule",
                            "job_name": job_name,
                            "message": f"Possible missed run for '{job_name}' - {gap_hours:.1f} hour gap between runs",
                            "details": f"Gap between {sorted_runs[i-1]['start_time']} and {sorted_runs[i]['start_time']}"
                        })
        
        return issues
    
    def _get_job_dependencies(self, job_name: str) -> Dict[str, Any]:
        """Get job dependencies (tables it loads, files it processes)"""
        # Get job details
        job_query = """
            SELECT job_id, job_description, job_type, schedule
            FROM jobs
            WHERE job_name = ?
        """
        
        if hasattr(self.data_service, 'connection_params'):
            job_query = job_query.replace('?', '%s')
        
        job_df = self.data_service.execute_query(job_query, {'job_name': job_name})
        
        if job_df.empty:
            return {}
        
        job_info = job_df.iloc[0].to_dict()
        
        # Get target tables
        tables_query = """
            SELECT DISTINCT jrt.table_name, jrt.schema_name
            FROM job_run_target_tables jrt
            JOIN job_runs jr ON jrt.job_run_id = jr.job_run_id
            JOIN jobs j ON jr.job_id = j.job_id
            WHERE j.job_name = ?
        """
        
        if hasattr(self.data_service, 'connection_params'):
            tables_query = tables_query.replace('?', '%s')
        
        tables_df = self.data_service.execute_query(tables_query, {'job_name': job_name})
        
        target_tables = []
        for _, row in tables_df.iterrows():
            target_tables.append({
                "schema": row['schema_name'],
                "table": row['table_name']
            })
        
        # Get typical source files
        files_query = """
            SELECT DISTINCT sf.file_name, sf.file_type
            FROM source_files sf
            JOIN job_run_source_files jrsf ON sf.file_id = jrsf.file_id
            JOIN job_runs jr ON jrsf.job_run_id = jr.job_run_id
            JOIN jobs j ON jr.job_id = j.job_id
            WHERE j.job_name = ?
            LIMIT 5
        """
        
        if hasattr(self.data_service, 'connection_params'):
            files_query = files_query.replace('?', '%s')
        
        files_df = self.data_service.execute_query(files_query, {'job_name': job_name})
        
        source_files = []
        for _, row in files_df.iterrows():
            source_files.append({
                "file_name": row['file_name'],
                "file_type": row['file_type']
            })
        
        return {
            "job_info": job_info,
            "target_tables": target_tables,
            "typical_source_files": source_files
        }