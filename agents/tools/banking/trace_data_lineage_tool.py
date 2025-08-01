from typing import Dict, Any, Optional, List
from agents.tools.base_tool import BaseTool
from services.data_interface import DataInterface


class TraceDataLineageTool(BaseTool):
    """Tool for tracing data lineage from views to source files"""
    
    def __init__(self, data_service: Optional[DataInterface] = None):
        super().__init__(
            name="TraceDataLineage",
            description="Trace the complete lineage of a view or table back to source files and jobs"
        )
        self.data_service = data_service
    
    def get_parameters(self) -> Dict[str, Dict[str, str]]:
        return {
            "object_name": {
                "type": "string",
                "description": "Name of the view or table to trace"
            },
            "object_type": {
                "type": "string",
                "description": "Type of object: 'view' or 'table'"
            }
        }
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Trace the complete lineage of a database object"""
        object_name = kwargs.get("object_name", "")
        object_type = kwargs.get("object_type", "view")
        
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
            lineage_chain = []
            processed_objects = set()
            
            # Start tracing from the given object
            objects_to_process = [(object_name, object_type)]
            
            while objects_to_process:
                current_object, current_type = objects_to_process.pop(0)
                
                # Skip if already processed
                if current_object in processed_objects:
                    continue
                processed_objects.add(current_object)
                
                if current_type == 'view':
                    # Get view dependencies
                    dependencies = self._get_view_dependencies(current_object)
                    
                    lineage_item = {
                        "object_name": current_object,
                        "object_type": "view",
                        "level": self._get_view_level(current_object),
                        "dependencies": dependencies,
                        "description": self._get_view_description(current_object)
                    }
                    lineage_chain.append(lineage_item)
                    
                    # Add dependencies to process queue
                    for dep in dependencies:
                        objects_to_process.append((dep['object_name'], dep['object_type']))
                
                elif current_type == 'table':
                    # Get jobs that load this table
                    jobs = self._get_table_source_jobs(current_object)
                    
                    lineage_item = {
                        "object_name": current_object,
                        "object_type": "table",
                        "source_jobs": jobs,
                        "last_loaded": self._get_table_last_loaded(current_object)
                    }
                    
                    # Get source files for each job
                    for job in jobs:
                        job['source_files'] = self._get_job_source_files(job['job_run_id'])
                    
                    lineage_chain.append(lineage_item)
            
            # Build lineage tree
            lineage_tree = self._build_lineage_tree(lineage_chain)
            
            # Get summary statistics
            summary = {
                "total_objects": len(lineage_chain),
                "views": sum(1 for item in lineage_chain if item['object_type'] == 'view'),
                "tables": sum(1 for item in lineage_chain if item['object_type'] == 'table'),
                "max_depth": max([item.get('level', 0) for item in lineage_chain if item['object_type'] == 'view'], default=0)
            }
            
            # Identify potential issues
            issues = self._identify_lineage_issues(lineage_chain)
            
            return {
                "success": True,
                "result": {
                    "root_object": object_name,
                    "lineage_chain": lineage_chain,
                    "lineage_tree": lineage_tree,
                    "summary": summary,
                    "potential_issues": issues
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Error tracing lineage for {object_name}: {str(e)}",
                "result": {}
            }
    
    def _get_view_dependencies(self, view_name: str) -> List[Dict[str, Any]]:
        """Get all dependencies for a view"""
        query = """
            SELECT 
                vd.depends_on_object as object_name,
                vd.depends_on_type as object_type,
                vd.depends_on_schema as schema_name
            FROM view_dependencies vd
            JOIN data_views dv ON vd.view_id = dv.view_id
            WHERE dv.view_name = :view_name
        """
        
        df = self.data_service.execute_query(query, {'view_name': view_name})
        
        dependencies = []
        for _, row in df.iterrows():
            dependencies.append({
                "object_name": row['object_name'],
                "object_type": row['object_type'],
                "schema_name": row['schema_name']
            })
        
        return dependencies
    
    def _get_view_level(self, view_name: str) -> int:
        """Get the level of a view in the dependency hierarchy"""
        query = """
            SELECT view_level
            FROM data_views
            WHERE view_name = :view_name
        """
        
        df = self.data_service.execute_query(query, {'view_name': view_name})
        
        if not df.empty:
            return int(df.iloc[0]['view_level'])
        return 0
    
    def _get_view_description(self, view_name: str) -> str:
        """Get the description of a view"""
        query = """
            SELECT description
            FROM data_views
            WHERE view_name = :view_name
        """
        
        df = self.data_service.execute_query(query, {'view_name': view_name})
        
        if not df.empty:
            return df.iloc[0]['description']
        return ""
    
    def _get_table_source_jobs(self, table_name: str) -> List[Dict[str, Any]]:
        """Get jobs that load data into a table"""
        query = """
            SELECT 
                jr.job_run_id,
                jr.job_id,
                j.job_name,
                jr.start_time,
                jr.end_time,
                jr.status,
                jrt.rows_inserted,
                jr.error_message
            FROM job_run_target_tables jrt
            JOIN job_runs jr ON jrt.job_run_id = jr.job_run_id
            JOIN jobs j ON jr.job_id = j.job_id
            WHERE LOWER(jrt.table_name) = LOWER(:table_name)
            ORDER BY jr.start_time DESC
            LIMIT 10
        """
        
        df = self.data_service.execute_query(query, {'table_name': table_name})
        
        jobs = []
        for _, row in df.iterrows():
            jobs.append({
                "job_run_id": row['job_run_id'],
                "job_id": row['job_id'],
                "job_name": row['job_name'],
                "start_time": str(row['start_time']),
                "end_time": str(row['end_time']) if row['end_time'] else None,
                "status": row['status'],
                "rows_inserted": int(row['rows_inserted']) if row['rows_inserted'] else 0,
                "error_message": row['error_message']
            })
        
        return jobs
    
    def _get_job_source_files(self, job_run_id: str) -> List[Dict[str, Any]]:
        """Get source files processed by a job run"""
        query = """
            SELECT 
                sf.file_id,
                sf.file_name,
                sf.file_path,
                sf.file_size,
                sf.arrival_time
            FROM job_run_source_files jrsf
            JOIN source_files sf ON jrsf.file_id = sf.file_id
            WHERE jrsf.job_run_id = :job_run_id
        """
        
        df = self.data_service.execute_query(query, {'job_run_id': job_run_id})
        
        files = []
        for _, row in df.iterrows():
            files.append({
                "file_id": row['file_id'],
                "file_name": row['file_name'],
                "file_path": row['file_path'],
                "file_size": int(row['file_size']) if row['file_size'] else 0,
                "arrival_time": str(row['arrival_time']) if row['arrival_time'] else None
            })
        
        return files
    
    def _get_table_last_loaded(self, table_name: str) -> Optional[str]:
        """Get the last time a table was successfully loaded"""
        query = """
            SELECT MAX(jr.end_time) as last_loaded
            FROM job_run_target_tables jrt
            JOIN job_runs jr ON jrt.job_run_id = jr.job_run_id
            WHERE LOWER(jrt.table_name) = LOWER(:table_name)
            AND jr.status = 'SUCCESS'
        """
        
        df = self.data_service.execute_query(query, {'table_name': table_name})
        
        if not df.empty and df.iloc[0]['last_loaded']:
            return str(df.iloc[0]['last_loaded'])
        return None
    
    def _build_lineage_tree(self, lineage_chain: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build a tree structure from the lineage chain"""
        # Find the root object (highest level view or the starting object)
        root = None
        max_level = -1
        
        for item in lineage_chain:
            if item['object_type'] == 'view':
                level = item.get('level', 0)
                if level > max_level:
                    max_level = level
                    root = item
        
        if not root and lineage_chain:
            root = lineage_chain[0]
        
        # Build tree recursively
        def build_node(obj_name: str, obj_type: str) -> Dict[str, Any]:
            # Find the object in lineage chain
            obj_info = None
            for item in lineage_chain:
                if item['object_name'] == obj_name and item['object_type'] == obj_type:
                    obj_info = item
                    break
            
            if not obj_info:
                return {"object_name": obj_name, "object_type": obj_type}
            
            node = {
                "object_name": obj_name,
                "object_type": obj_type,
                "details": obj_info
            }
            
            # Add children
            if obj_type == 'view' and 'dependencies' in obj_info:
                node['dependencies'] = []
                for dep in obj_info['dependencies']:
                    child = build_node(dep['object_name'], dep['object_type'])
                    node['dependencies'].append(child)
            elif obj_type == 'table' and 'source_jobs' in obj_info:
                node['source_jobs'] = obj_info['source_jobs']
            
            return node
        
        if root:
            return build_node(root['object_name'], root['object_type'])
        return {}
    
    def _identify_lineage_issues(self, lineage_chain: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """Identify potential issues in the lineage"""
        issues = []
        
        # Check for failed jobs
        for item in lineage_chain:
            if item['object_type'] == 'table' and 'source_jobs' in item:
                for job in item['source_jobs']:
                    if job['status'] == 'FAILED':
                        issues.append({
                            "severity": "HIGH",
                            "type": "Failed Job",
                            "message": f"Job '{job['job_name']}' failed at {job['start_time']} loading {item['object_name']}",
                            "details": job.get('error_message', 'No error message available')
                        })
        
        # Check for stale data
        from datetime import datetime, timedelta
        current_time = datetime.now()
        
        for item in lineage_chain:
            if item['object_type'] == 'table' and 'last_loaded' in item:
                if item['last_loaded']:
                    last_loaded = datetime.fromisoformat(item['last_loaded'].replace('Z', '+00:00'))
                    age_hours = (current_time - last_loaded).total_seconds() / 3600
                    
                    if age_hours > 48:
                        issues.append({
                            "severity": "MEDIUM",
                            "type": "Stale Data",
                            "message": f"Table {item['object_name']} hasn't been updated in {int(age_hours)} hours",
                            "details": f"Last successful load: {item['last_loaded']}"
                        })
        
        # Check for missing dependencies
        for item in lineage_chain:
            if item['object_type'] == 'view' and 'dependencies' in item:
                if len(item['dependencies']) == 0 and item.get('level', 0) > 1:
                    issues.append({
                        "severity": "LOW",
                        "type": "Missing Dependencies",
                        "message": f"View {item['object_name']} has no registered dependencies",
                        "details": "This might indicate incomplete lineage tracking"
                    })
        
        return issues