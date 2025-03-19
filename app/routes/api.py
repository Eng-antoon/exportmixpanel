from flask import request, jsonify
from app import app, db_session, executor, job_status
import uuid
import threading
from db.models import Trip
from app.models.operations import update_trip_db, migrate_db

@app.route("/api/update_db", methods=["POST"])
def update_db():
    """
    Update the database with information about a specific trip.
    """
    try:
        trip_id = request.form.get("trip_id")
        force_update = request.form.get("force_update", "false").lower() == "true"
        
        if not trip_id:
            return jsonify({"success": False, "message": "Trip ID is required"})
            
        result = update_trip_db(int(trip_id), force_update)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Error: {str(e)}"})

@app.route("/api/update_db_async", methods=["POST"])
def update_db_async():
    """
    Update a trip asynchronously and track progress.
    """
    trip_id = request.form.get("trip_id")
    force_update = request.form.get("force_update", "false").lower() == "true"
    
    if not trip_id:
        return jsonify({"success": False, "message": "Trip ID is required"})
        
    # Generate a unique job ID
    job_id = str(uuid.uuid4())
    
    # Initialize job status in the jobs dictionary
    job_status[job_id] = {
        "status": "starting",
        "progress": 0,
        "message": "Starting job...",
        "completed": 0,
        "total": 1,
        "success_count": 0,
        "error_count": 0
    }
    
    # Submit the job to the executor
    executor.submit(process_update_db_async, job_id, int(trip_id), force_update)
    
    return jsonify({
        "success": True,
        "message": "Job started",
        "job_id": job_id
    })

def process_update_db_async(job_id, trip_id, force_update=False):
    """
    Process a single trip update asynchronously.
    
    Args:
        job_id: Unique ID for tracking this job
        trip_id: ID of the trip to update
        force_update: Whether to force the update
    """
    try:
        # Update job status
        job_status[job_id]["status"] = "processing"
        job_status[job_id]["message"] = f"Processing trip {trip_id}..."
        
        # Perform the update
        result = update_trip_db(trip_id, force_update)
        
        # Update counts based on result
        if result["success"]:
            job_status[job_id]["success_count"] += 1
        else:
            job_status[job_id]["error_count"] += 1
            
        job_status[job_id]["completed"] += 1
        job_status[job_id]["progress"] = 100  # 100% complete
        
        # Set final status
        job_status[job_id]["status"] = "completed"
        job_status[job_id]["message"] = "Job completed successfully"
        
    except Exception as e:
        # Handle errors
        job_status[job_id]["status"] = "error"
        job_status[job_id]["message"] = f"Error: {str(e)}"
        job_status[job_id]["error_count"] += 1
        job_status[job_id]["completed"] += 1

@app.route("/api/update_all_db_async", methods=["POST"])
def update_all_db_async():
    """
    Update all trips in the database asynchronously.
    """
    force_update = request.form.get("force_update", "false").lower() == "true"
    
    # Generate a unique job ID
    job_id = str(uuid.uuid4())
    
    # Submit the job to the executor
    executor.submit(process_update_all_db_async, job_id, force_update)
    
    return jsonify({
        "success": True,
        "message": "Job started",
        "job_id": job_id
    })

def process_update_all_db_async(job_id, force_update=False):
    """
    Process updates for all trips asynchronously.
    
    Args:
        job_id: Unique ID for tracking this job
        force_update: Whether to force the update
    """
    try:
        # Get all trip IDs from the database
        trips = db_session.query(Trip).all()
        trip_ids = [trip.trip_id for trip in trips]
        total_trips = len(trip_ids)
        
        # Initialize job status
        job_status[job_id] = {
            "status": "processing",
            "progress": 0,
            "message": f"Processing {total_trips} trips...",
            "completed": 0,
            "total": total_trips,
            "success_count": 0,
            "error_count": 0
        }
        
        # Define a lock for thread-safe updating of job status
        lock = threading.Lock()
        
        def update_single_trip(trip_id):
            try:
                # Update the trip
                result = update_trip_db(trip_id, force_update)
                
                # Update job status with lock to avoid race conditions
                with lock:
                    if result["success"]:
                        job_status[job_id]["success_count"] += 1
                    else:
                        job_status[job_id]["error_count"] += 1
                        
                    job_status[job_id]["completed"] += 1
                    progress = round(100 * job_status[job_id]["completed"] / job_status[job_id]["total"])
                    job_status[job_id]["progress"] = progress
                    job_status[job_id]["message"] = f"Processed {job_status[job_id]['completed']} of {total_trips} trips..."
                    
            except Exception as e:
                with lock:
                    job_status[job_id]["error_count"] += 1
                    job_status[job_id]["completed"] += 1
        
        # Submit all trips to the executor
        futures = []
        for trip_id in trip_ids:
            future = executor.submit(update_single_trip, trip_id)
            futures.append(future)
            
        # Wait for all futures to complete
        for future in futures:
            future.result()
            
        # Set final status
        job_status[job_id]["status"] = "completed"
        job_status[job_id]["progress"] = 100
        job_status[job_id]["message"] = f"Job completed. Successful: {job_status[job_id]['success_count']}, Errors: {job_status[job_id]['error_count']}"
        
    except Exception as e:
        # Handle errors
        job_status[job_id]["status"] = "error"
        job_status[job_id]["message"] = f"Error: {str(e)}"

@app.route("/api/update_progress", methods=["GET"])
def update_progress():
    """
    Get the progress of an asynchronous update job.
    """
    job_id = request.args.get("job_id")
    
    if not job_id or job_id not in job_status:
        return jsonify({
            "success": False,
            "message": "Invalid or expired job ID"
        })
        
    current_job_status = job_status[job_id]
    
    # Clean up completed jobs that are older than 5 minutes
    if current_job_status["status"] in ["completed", "error"]:
        # In a real app, you might want to add a timestamp to jobs and clean up old ones
        pass
        
    return jsonify({
        "success": True,
        "status": current_job_status["status"],
        "progress": current_job_status["progress"],
        "message": current_job_status["message"],
        "completed": current_job_status["completed"],
        "total": current_job_status["total"],
        "success_count": current_job_status.get("success_count", 0),
        "error_count": current_job_status.get("error_count", 0)
    }) 