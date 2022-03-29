ALTER TABLE jobs ADD COLUMN IF NOT EXISTS parent_job_id uuid CONSTRAINT jobs_parent_fk_jobs REFERENCES jobs (uuid);
ALTER TABLE runs ADD COLUMN IF NOT EXISTS parent_run_id uuid CONSTRAINT runs_parent_fk_runs REFERENCES runs (uuid);
DROP INDEX IF EXISTS jobs_name_index;
CREATE UNIQUE INDEX IF NOT EXISTS jobs_name_parent ON jobs (name, namespace_name, parent_job_id) INCLUDE (uuid, name, namespace_name, parent_job_id);

CREATE OR REPLACE VIEW jobs_view
AS
    with recursive job_fqn AS (
        SELECT uuid, name, namespace_name, parent_job_id
        FROM jobs
        UNION ALL
        SELECT j1.uuid,
               CASE WHEN j2.name IS NOT NULL THEN j2.name || '.' || j1.name ELSE j1.name END AS name,
               CASE WHEN j2.namespace_name IS NOT NULL THEN j2.namespace_name ELSE j1.namespace_name END AS namespace_name,
               j2.parent_job_id
        FROM jobs j1
        INNER JOIN job_fqn j2 ON j2.uuid=j1.parent_job_id
    )
    SELECT f.uuid,
           f.name,
           f.namespace_name,
           j.name AS simple_name,
           j.parent_job_id,
           j.type,
           j.created_at,
           j.updated_at,
           j.namespace_uuid,
           j.description,
           j.current_version_uuid,
           j.current_job_context_uuid,
           j.current_location,
           j.current_inputs
           FROM job_fqn f, jobs j WHERE j.uuid=f.uuid
