// assets/sidebar-toggle.js

// Method 1: Use MutationObserver to wait for Dash to render
function initSidebarToggle() {
  let header = document.querySelector('.header');
  if (!header) {
    console.warn('Sidebar toggle: .header element not found.');
    return;
  }

  // Check if button already exists
  if (document.getElementById('sidebar-toggle-js')) {
    return;
  }

  // Create hamburger button
  let btn = document.createElement('button');
  btn.id = 'sidebar-toggle-js';
  btn.className = 'sidebar-toggle-js';
  btn.setAttribute('aria-label', 'Toggle sidebar');
  btn.innerHTML = '&#9776;';
  header.insertBefore(btn, header.firstChild);

  btn.addEventListener('click', function () {
    let sidebar = document.querySelector('.sidebar');
    if (!sidebar) {
      console.warn('Sidebar element not found.');
      return;
    }
    
    sidebar.classList.toggle('collapsed');
    
    // Adjust grid layout
    const appGrid = document.querySelector('.app-grid');
    if (appGrid) {
      if (sidebar.classList.contains('collapsed')) {
        appGrid.style.gridTemplateColumns = '0fr 1fr';
      } else {
        appGrid.style.gridTemplateColumns = '1.3fr 6.2fr';
      }
    }
  });
}

// Wait for Dash to fully render
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', function() {
    // Dash usually takes a bit longer, so delay initialization
    setTimeout(initSidebarToggle, 1000);
  });
} else {
  setTimeout(initSidebarToggle, 1000);
}

// Additional safety: re-init when Dash updates the DOM
const observer = new MutationObserver(function(mutations) {
  mutations.forEach(function(mutation) {
    if (!document.getElementById('sidebar-toggle-js') && document.querySelector('.header')) {
      initSidebarToggle();
    }
  });
});

observer.observe(document.body, { childList: true, subtree: true });