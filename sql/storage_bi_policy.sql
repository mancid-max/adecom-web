-- Bucket privado 'bi': los JSON del dashboard ADECOM.
-- Pegar en Supabase → SQL Editor → Run.  (El bucket ya fue creado por scripts/subir_json_supabase.py)
--
-- Regla: cualquier usuario AUTENTICADO puede leer; nadie puede escribir desde el navegador
-- (la escritura la hace auto_build.bat con la service key, que ignora RLS).

drop policy if exists "bi_read_authenticated" on storage.objects;

create policy "bi_read_authenticated"
on storage.objects for select
to authenticated
using (bucket_id = 'bi');
