// Main JavaScript for Task System

/**
 * Show toast notification
 * @param {string} message - Message to display
 * @param {string} type - Type: 'success', 'error', 'info', 'warning'
 */
function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = `alert alert-${type} shadow-lg`;
    toast.innerHTML = `
        <div>
            <span>${message}</span>
        </div>
    `;

    container.appendChild(toast);

    // Auto-remove after 3 seconds
    setTimeout(() => {
        toast.style.opacity = '0';
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

/**
 * Confirm action before proceeding
 * @param {string} message - Confirmation message
 * @returns {boolean}
 */
function confirmAction(message) {
    return confirm(message);
}

/**
 * Format date to readable string
 * @param {string} dateString - ISO date string
 * @returns {string}
 */
function formatDate(dateString) {
    if (!dateString) return '-';
    const date = new Date(dateString);
    return date.toLocaleString();
}

/**
 * Copy text to clipboard
 * @param {string} text - Text to copy
 */
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(() => {
        showToast('Copied to clipboard!', 'success');
    }).catch(() => {
        showToast('Failed to copy', 'error');
    });
}

/**
 * Validate JSON input
 * @param {string} jsonString - JSON string to validate
 * @returns {boolean}
 */
function isValidJSON(jsonString) {
    try {
        JSON.parse(jsonString);
        return true;
    } catch (e) {
        return false;
    }
}

// HTMX event handlers
document.addEventListener('htmx:responseError', function(event) {
    showToast('Request failed. Please try again.', 'error');
});

document.addEventListener('htmx:sendError', function(event) {
    showToast('Network error. Check your connection.', 'error');
});

// Form validation for JSON fields
document.addEventListener('submit', function(event) {
    const form = event.target;
    const jsonFields = form.querySelectorAll('textarea[name="payload_data"], textarea[name="timeouts"]');

    jsonFields.forEach(field => {
        if (field.value && !isValidJSON(field.value)) {
            event.preventDefault();
            showToast(`Invalid JSON in ${field.name}`, 'error');
            field.classList.add('input-error');
            field.focus();
        }
    });
});

// Global keyboard shortcuts
document.addEventListener('keydown', function(event) {
    // Ctrl/Cmd + K - Focus search (if exists)
    if ((event.ctrlKey || event.metaKey) && event.key === 'k') {
        event.preventDefault();
        const searchInput = document.querySelector('input[type="search"]');
        if (searchInput) searchInput.focus();
    }

    // Escape - Close modal
    if (event.key === 'Escape') {
        const modalContainer = document.getElementById('modal-container');
        if (modalContainer) {
            modalContainer.innerHTML = '';
        }
    }
});

// Initialize tooltips (if using any library)
document.addEventListener('DOMContentLoaded', function() {
    console.log('Task System UI loaded');
});

// Expose functions globally
window.showToast = showToast;
window.confirmAction = confirmAction;
window.formatDate = formatDate;
window.copyToClipboard = copyToClipboard;
window.isValidJSON = isValidJSON;