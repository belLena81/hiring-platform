#!/bin/sh
# Container entrypoint: for every NAME_FILE variable that points under /run/secrets/, export NAME with the file's
# content (trailing newlines removed) and drop NAME_FILE, then replace this shell with the given command.
# Values never appear in container metadata or in this script's output. Other *_FILE variables, including paths with
# "..", are left untouched; an empty, missing or unreadable secret file, or NAME set alongside NAME_FILE, stops startup.
set -eu

secret_root=/run/secrets/

for file_variable in $(env | sed -n 's/^\([A-Z][A-Z0-9_]*\)_FILE=.*$/\1/p'); do
  eval "secret_path=\${${file_variable}_FILE}"
  case "$secret_path" in
    *..*) continue ;;
    "$secret_root"*) ;;
    *) continue ;;
  esac
  eval "already_set=\${${file_variable}+set}"
  if [ "${already_set:-}" = set ]; then
    echo "load-secret-files: both ${file_variable} and ${file_variable}_FILE are set" >&2
    exit 64
  fi
  if [ ! -f "$secret_path" ] || [ ! -r "$secret_path" ]; then
    echo "load-secret-files: ${file_variable}_FILE does not name a readable file" >&2
    exit 66
  fi
  secret_value=$(cat "$secret_path")
  if [ -z "$secret_value" ]; then
    echo "load-secret-files: ${file_variable}_FILE names an empty file" >&2
    exit 65
  fi
  export "${file_variable}=${secret_value}"
  unset "${file_variable}_FILE"
done
unset secret_value secret_path already_set file_variable secret_root

exec "$@"
