function formatJsonAsHtml(data, preview = false) {
    let jsonStr;
    try {
        jsonStr = JSON.stringify(
            typeof data === 'object' ? data : JSON.parse(data),
            null, 2
        );
    } catch (e) {
        jsonStr = String(data);
    }

    if (preview) {
        jsonStr = jsonStr.length > 120 ? jsonStr.substring(0, 120) + '…' : jsonStr;
    }

    const escaped = escapeHtml(jsonStr);

    return preview
        ? `<small class="text-muted">${escaped}</small>`
        : `<pre class="mb-0 bg-light p-2 rounded small text-break"
                style="white-space: pre-wrap; word-wrap: break-word; max-height: 300px; overflow-y: auto;">
${escaped}</pre>`;
}

function escapeHtml(str) {
    return str
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}