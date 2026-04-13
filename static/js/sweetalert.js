function notify(type, msg) {
    // Toastr-like style for SweetAlert2
    Swal.fire({
        toast: true,
        position: 'top-end',
        icon: type,
        title: msg,
        showConfirmButton: false,
        timer: 3500,
        timerProgressBar: true,
        customClass: {
            popup: 'swal2-toastr-popup'
        },
        didOpen: (toast) => {
            toast.addEventListener('mouseenter', Swal.stopTimer);
            toast.addEventListener('mouseleave', Swal.resumeTimer);
        }
    });
}

// Toastr-like confirm (simple, top, no modal background)
function confirmAction(title, text, callback) {
    Swal.fire({
        title: title,
        text: text,
        icon: 'warning',
        toast: true,
        position: 'top-end',
        showCancelButton: true,
        confirmButtonColor: '#3085d6',
        cancelButtonColor: '#d33',
        confirmButtonText: 'Sim',
        cancelButtonText: 'Cancelar',
        customClass: {
            popup: 'swal2-toastr-popup'
        },
        background: '#fff',
        showCloseButton: true,
        showConfirmButton: true,
        showCancelButton: true,
        allowOutsideClick: true,
        allowEscapeKey: true,
        allowEnterKey: true
    }).then((result) => {
        if (result.isConfirmed) {
            callback();
        }
    });
}

// Optional: Add some custom CSS for a more Toastr-like look
if (!document.getElementById('swal2-toastr-style')) {
    const style = document.createElement('style');
    style.id = 'swal2-toastr-style';
    style.innerHTML = `
    .swal2-toastr-popup {
        min-width: 320px !important;
        max-width: 400px !important;
        box-shadow: 0 2px 12px rgba(0,0,0,0.15) !important;
        border-radius: 6px !important;
        padding: 0.75rem 1.25rem !important;
        font-size: 1rem !important;
    }
    .swal2-toast .swal2-title {
        margin: 0 !important;
        font-size: 1rem !important;
        font-weight: 500 !important;
    }
    `;
    document.head.appendChild(style);
}
