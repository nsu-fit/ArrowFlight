#!/bin/sh
set -eu

ubuntu_mirror="${UBUNTU_MIRROR:-http://mirror.yandex.ru/ubuntu}"
ubuntu_mirror="${ubuntu_mirror%/}"

case "${ubuntu_mirror}" in
    http://*|https://*) ;;
    *)
        echo "UBUNTU_MIRROR must be an HTTP(S) URL: ${ubuntu_mirror}" >&2
        exit 2
        ;;
esac

sources_found=false
for sources_file in \
    /etc/apt/sources.list \
    /etc/apt/sources.list.d/ubuntu.sources
do
    if [ ! -f "${sources_file}" ]; then
        continue
    fi

    sources_found=true
    sed -i \
        -e "s|http://archive.ubuntu.com/ubuntu|${ubuntu_mirror}|g" \
        -e "s|https://archive.ubuntu.com/ubuntu|${ubuntu_mirror}|g" \
        -e "s|http://security.ubuntu.com/ubuntu|${ubuntu_mirror}|g" \
        -e "s|https://security.ubuntu.com/ubuntu|${ubuntu_mirror}|g" \
        "${sources_file}"
done

if [ "${sources_found}" = false ]; then
    echo "No Ubuntu APT sources file found; leaving repositories unchanged." >&2
fi

printf '%s\n' \
    'Acquire::Retries "5";' \
    'Acquire::http::Timeout "20";' \
    'Acquire::https::Timeout "20";' \
    > /etc/apt/apt.conf.d/80arrowflight-network

echo "==> Ubuntu APT mirror: ${ubuntu_mirror}"
