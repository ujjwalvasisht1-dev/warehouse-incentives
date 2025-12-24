// Supervisor Dashboard JavaScript

let currentFilter = 'today';
let currentCohort = 1;

// Initialize dashboard
document.addEventListener('DOMContentLoaded', function() {
    // Cohort tab setup
    const cohortTabs = document.querySelectorAll('.cohort-tab');
    cohortTabs.forEach(tab => {
        tab.addEventListener('click', function() {
            currentCohort = parseInt(this.dataset.cohort);
            cohortTabs.forEach(t => t.classList.remove('active'));
            this.classList.add('active');
            updateCohortHeader();
            loadRankings();
            updateDownloadLink();
        });
    });
    
    // Time filter setup
    const filterButtons = document.querySelectorAll('.filter-btn');
    filterButtons.forEach(btn => {
        btn.addEventListener('click', function() {
            currentFilter = this.dataset.filter;
            filterButtons.forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            loadRankings();
            updateDownloadLink();
        });
    });
    
    // Modal setup
    const modal = document.getElementById('picker-modal');
    const closeBtn = document.querySelector('.close');
    
    if (closeBtn) {
        closeBtn.addEventListener('click', function() {
            modal.style.display = 'none';
        });
    }
    
    window.addEventListener('click', function(event) {
        if (event.target === modal) {
            modal.style.display = 'none';
        }
    });
    
    // Initial load
    updateCohortHeader();
    loadRankings();
    updateDownloadLink();
    
    // Auto-refresh every 30 seconds
    setInterval(loadRankings, 30000);
});

function updateCohortHeader() {
    const title = document.getElementById('cohort-title');
    if (title) {
        title.textContent = `Cohort ${currentCohort} Rankings`;
    }
}

function updateDownloadLink() {
    const downloadLink = document.getElementById('download-link');
    if (downloadLink) {
        downloadLink.href = `/supervisor/download?filter=${currentFilter}&cohort=${currentCohort}`;
    }
}

function loadRankings() {
    const tbody = document.getElementById('rankings-body');
    if (tbody) {
        tbody.innerHTML = '<tr><td colspan="8" class="loading">Loading rankings...</td></tr>';
    }
    
    fetch(`/supervisor/api/rankings?filter=${currentFilter}&cohort=${currentCohort}`)
        .then(response => response.json())
        .then(data => {
            updateRankingsTable(data);
            updateCohortStats(data);
        })
        .catch(error => {
            console.error('Error loading rankings:', error);
            if (tbody) {
                tbody.innerHTML = '<tr><td colspan="8" class="loading">Error loading data. Please refresh.</td></tr>';
            }
        });
}

function updateRankingsTable(data) {
    const tbody = document.getElementById('rankings-body');
    if (!tbody) return;
    
    if (!data.rankings || data.rankings.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="loading">No data available for this cohort</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.rankings.map(picker => `
        <tr>
            <td><strong>#${picker.rank}</strong></td>
            <td>${picker.picker_id}</td>
            <td>${picker.unique_picklists}</td>
            <td>${picker.items_picked}</td>
            <td>${picker.items_lost}</td>
            <td><strong>${picker.score}</strong></td>
            <td><span class="rank-badge ${picker.status_color}">${getStatusLabel(picker.status_color)}</span></td>
            <td><button class="btn-view" onclick="viewPickerDetails('${picker.picker_id}')">View Details</button></td>
        </tr>
    `).join('');
}

function updateCohortStats(data) {
    const pickersEl = document.getElementById('cohort-pickers');
    const avgEl = document.getElementById('cohort-avg');
    
    if (pickersEl) {
        pickersEl.textContent = data.total_pickers || 0;
    }
    if (avgEl) {
        avgEl.textContent = data.daily_avg || '-';
    }
}

function getStatusLabel(color) {
    if (color === 'green') return 'Going Good';
    if (color === 'yellow') return 'Can Do Better';
    return 'Need to Perform Better';
}

function viewPickerDetails(pickerId) {
    const modal = document.getElementById('picker-modal');
    const modalPickerId = document.getElementById('modal-picker-id');
    const modalContent = document.getElementById('modal-content');
    
    if (!modal || !modalPickerId || !modalContent) return;
    
    modalPickerId.textContent = `Picker Details: ${pickerId}`;
    modalContent.innerHTML = '<div class="loading">Loading details...</div>';
    modal.style.display = 'block';
    
    fetch(`/supervisor/api/picker/${pickerId}?filter=${currentFilter}`)
        .then(response => response.json())
        .then(data => {
            if (data.details.length === 0) {
                modalContent.innerHTML = '<p>No details available for this picker.</p>';
                return;
            }
            
            // Group by picklist
            const picklists = {};
            data.details.forEach(item => {
                const picklistId = item.external_picklist_id;
                if (!picklists[picklistId]) {
                    picklists[picklistId] = [];
                }
                picklists[picklistId].push(item);
            });
            
            let html = '<div style="max-height: 500px; overflow-y: auto;">';
            html += `<p><strong>Total Picklists:</strong> ${Object.keys(picklists).length}</p>`;
            html += '<table class="detail-table">';
            html += '<thead><tr><th>Picklist ID</th><th>Location</th><th>Status</th><th>Updated At</th></tr></thead>';
            html += '<tbody>';
            
            Object.keys(picklists).forEach(picklistId => {
                picklists[picklistId].forEach((item, idx) => {
                    if (idx === 0) {
                        html += `<tr><td rowspan="${picklists[picklistId].length}">${picklistId}</td>`;
                    } else {
                        html += '<tr>';
                    }
                    html += `<td>${item.location_bin_id}</td>`;
                    html += `<td><span class="rank-badge ${getStatusColorForItem(item.item_status)}">${item.item_status}</span></td>`;
                    html += `<td>${new Date(item.updated_at).toLocaleString()}</td></tr>`;
                });
            });
            
            html += '</tbody></table></div>';
            modalContent.innerHTML = html;
        })
        .catch(error => {
            console.error('Error loading picker details:', error);
            modalContent.innerHTML = '<p>Error loading details. Please try again.</p>';
        });
}

function getStatusColorForItem(status) {
    if (status === 'COMPLETED' || status === 'ITEM_REPLACED') return 'green';
    if (status === 'ITEM_NOT_FOUND') return 'red';
    return 'yellow';
}
