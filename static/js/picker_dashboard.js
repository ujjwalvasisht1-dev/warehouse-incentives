// Picker Dashboard JavaScript

let currentFilter = 'today';

// Initialize dashboard
document.addEventListener('DOMContentLoaded', function() {
    // Set active filter button
    const filterButtons = document.querySelectorAll('.filter-btn');
    filterButtons.forEach(btn => {
        if (btn.dataset.filter === currentFilter) {
            btn.classList.add('active');
        }
        
        btn.addEventListener('click', function() {
            currentFilter = this.dataset.filter;
            filterButtons.forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            loadStats();
        });
    });
    
    // Load initial stats
    loadStats();
    
    // Auto-refresh every 30 seconds
    setInterval(loadStats, 30000);
});

function loadStats() {
    const loadingEl = document.getElementById('loading');
    if (loadingEl) {
        loadingEl.style.display = 'block';
    }
    
    fetch(`/picker/api/stats?filter=${currentFilter}`)
        .then(response => response.json())
        .then(data => {
            updateDashboard(data);
            if (loadingEl) {
                loadingEl.style.display = 'none';
            }
        })
        .catch(error => {
            console.error('Error loading stats:', error);
            if (loadingEl) {
                loadingEl.textContent = 'Error loading data. Please refresh.';
            }
        });
}

function updateDashboard(data) {
    // Update stats
    document.getElementById('items-picked').textContent = data.items_picked || 0;
    document.getElementById('items-lost').textContent = data.items_lost || 0;
    document.getElementById('score').textContent = data.score || 0;
    
    // Update ranking
    const rankEl = document.getElementById('rank');
    const totalPickersEl = document.getElementById('total-pickers');
    const rankTotalSpan = document.querySelector('.rank-total');
    
    if (data.rank === 0 || data.items_picked === 0) {
        // Not ranked - no data for this period
        rankEl.textContent = 'NR';
        rankEl.title = 'Not ranked - no items picked in this period';
        rankEl.style.fontSize = '36px';
        if (rankTotalSpan) {
            rankTotalSpan.innerHTML = `<span style="color: #999;">Not Ranked</span><br><span style="font-size: 14px;">${data.total_pickers || 0} active pickers</span>`;
        }
    } else {
        rankEl.textContent = data.rank;
        rankEl.style.fontSize = '';
        if (rankTotalSpan) {
            rankTotalSpan.innerHTML = `of <span id="total-pickers">${data.total_pickers || '-'}</span> pickers`;
        }
    }
    
    document.getElementById('items-to-next').textContent = data.items_to_next_rank || '-';
    document.getElementById('diff-from-first').textContent = data.difference_from_first || '-';
    document.getElementById('daily-avg').textContent = data.daily_avg || '-';
    
    // Update status badge
    const statusBadge = document.getElementById('status-badge');
    const statusText = document.getElementById('status-text');
    
    if (statusBadge && statusText) {
        statusBadge.className = `status-badge ${data.status_color}`;
        
        let statusLabel = '';
        if (data.status_color === 'green') {
            statusLabel = 'Going Good';
        } else if (data.status_color === 'yellow') {
            statusLabel = 'Can Do Better';
        } else {
            statusLabel = 'Need to Perform Better';
        }
        
        statusText.textContent = statusLabel;
    }
    
    // Update unique picklists count
    const picklistsEl = document.getElementById('unique-picklists');
    if (picklistsEl) {
        picklistsEl.textContent = data.unique_picklists || 0;
    }
    
    // Update incentive banner
    updateIncentiveBanner(data.rank, data.total_pickers, data.items_to_next_rank, data.leaderboard);
    
    // Update leaderboard table (top 15)
    updateLeaderboard(data.leaderboard || [], data.current_user_entry);
}

function updateIncentiveBanner(rank, totalPickers, itemsToNext, leaderboard) {
    // Check if picker is not ranked (no data for this period)
    const isNotRanked = rank === 0 || rank === null || rank === undefined;
    
    // Determine current tier and next tier
    let currentTier = 0;
    let nextTierRank = 0;
    let nextTierName = '';
    
    if (isNotRanked) {
        currentTier = 0;
        nextTierRank = 50;
        nextTierName = 'Top 50';
    } else if (rank <= 3) {
        currentTier = 1;
        nextTierRank = 0; // Already at top!
        nextTierName = 'Top 3';
    } else if (rank <= 10) {
        currentTier = 2;
        nextTierRank = 3;
        nextTierName = 'Top 3';
    } else if (rank <= 25) {
        currentTier = 3;
        nextTierRank = 10;
        nextTierName = 'Top 10';
    } else if (rank <= 50) {
        currentTier = 4;
        nextTierRank = 25;
        nextTierName = 'Top 25';
    } else {
        currentTier = 0;
        nextTierRank = 50;
        nextTierName = 'Top 50';
    }
    
    // Highlight active tier
    document.querySelectorAll('.tier').forEach(tier => tier.classList.remove('active'));
    if (currentTier > 0) {
        const activeTier = document.querySelector(`.tier[data-tier="${currentTier}"]`);
        if (activeTier) activeTier.classList.add('active');
    }
    
    // Update motivation message
    const messageEl = document.getElementById('motivation-message');
    if (messageEl) {
        let message = '';
        let emoji1 = '';
        let emoji2 = '';
        
        if (isNotRanked) {
            emoji1 = '🚀';
            emoji2 = '💪';
            message = "You haven't picked any items in this period yet. Start picking to get on the leaderboard!";
        } else if (rank === 1) {
            emoji1 = '👑';
            emoji2 = '🎉';
            message = "YOU'RE #1! Amazing work, Champion! Keep defending your throne!";
        } else if (rank <= 3) {
            emoji1 = '🏆';
            emoji2 = '🔥';
            message = `Incredible! You're in the TOP 3! Just ${itemsToNext || 0} more items to reach #${rank - 1}!`;
        } else if (rank <= 10) {
            emoji1 = '💪';
            emoji2 = '🚀';
            const itemsToTop3 = getItemsToRank(leaderboard, rank, 3);
            message = `Great job! You're in TOP 10! Pick ${itemsToTop3} more items to enter TOP 3!`;
        } else if (rank <= 25) {
            emoji1 = '⚡';
            emoji2 = '📈';
            const itemsToTop10 = getItemsToRank(leaderboard, rank, 10);
            message = `You're making progress! Pick ${itemsToTop10} more items to enter TOP 10!`;
        } else if (rank <= 50) {
            emoji1 = '🎯';
            emoji2 = '💫';
            const itemsToTop25 = getItemsToRank(leaderboard, rank, 25);
            message = `Keep pushing! Pick ${itemsToTop25} more items to enter TOP 25!`;
        } else {
            emoji1 = '🌟';
            emoji2 = '💪';
            const itemsToTop50 = getItemsToRank(leaderboard, rank, 50);
            message = `Every item counts! Pick ${itemsToTop50} more items to enter TOP 50!`;
        }
        
        messageEl.innerHTML = `
            <div class="message-text">
                <span class="message-emoji">${emoji1}</span>
                ${message}
                <span class="message-emoji">${emoji2}</span>
            </div>
        `;
    }
    
    // Update progress bar
    const progressBar = document.getElementById('tier-progress-bar');
    const progressInfo = document.getElementById('progress-info');
    const progressSection = document.getElementById('progress-to-next');
    
    if (progressBar && progressInfo && progressSection) {
        if (isNotRanked) {
            // Not ranked - no progress
            progressBar.style.width = '0%';
            progressInfo.textContent = '📋 Pick items to start climbing the leaderboard!';
        } else if (rank <= 3) {
            // Already at top tier
            progressBar.style.width = '100%';
            progressInfo.textContent = '🎊 You\'re in the highest tier! Keep it up!';
        } else {
            // Calculate progress to next tier
            let tierStart, tierEnd;
            if (rank <= 10) {
                tierStart = 10; tierEnd = 3;
            } else if (rank <= 25) {
                tierStart = 25; tierEnd = 10;
            } else if (rank <= 50) {
                tierStart = 50; tierEnd = 25;
            } else {
                tierStart = totalPickers; tierEnd = 50;
            }
            
            const progressPct = Math.max(0, Math.min(100, ((tierStart - rank) / (tierStart - tierEnd)) * 100));
            progressBar.style.width = progressPct + '%';
            
            const positionsNeeded = rank - tierEnd;
            progressInfo.textContent = `${positionsNeeded} position${positionsNeeded > 1 ? 's' : ''} away from ${nextTierName}`;
        }
    }
}

function getItemsToRank(leaderboard, currentRank, targetRank) {
    if (!leaderboard || leaderboard.length === 0) return '?';
    
    const currentPicker = leaderboard.find(p => p.rank === currentRank);
    const targetPicker = leaderboard.find(p => p.rank === targetRank);
    
    if (!currentPicker || !targetPicker) return '?';
    
    const diff = targetPicker.score - currentPicker.score + 1;
    return diff > 0 ? diff : 1;
}

function updateLeaderboard(leaderboard, currentUserEntry) {
    const tbody = document.getElementById('leaderboard-body');
    const currentUserSection = document.getElementById('current-user-section');
    const currentUserBody = document.getElementById('current-user-body');
    
    if (!tbody) return;
    
    if (leaderboard.length === 0) {
        tbody.innerHTML = '<tr><td colspan="9" class="loading">No data available</td></tr>';
        if (currentUserSection) currentUserSection.style.display = 'none';
        return;
    }
    
    // Check if current user is in the top 15 (leaderboard)
    const currentUserInTop15 = leaderboard.some(p => p.is_current_user);
    
    // Render leaderboard (top 15)
    tbody.innerHTML = leaderboard.map(picker => {
        const rowClass = picker.is_current_user ? 'current-user-row' : '';
        const statusLabel = getStatusLabel(picker.status_color);
        const displayName = picker.name || '-';
        const ageDisplay = picker.age_in_days !== null && picker.age_in_days !== undefined ? picker.age_in_days : '-';
        
        return `
            <tr class="${rowClass}">
                <td><strong>#${picker.rank}</strong></td>
                <td>${displayName}</td>
                <td>${picker.picker_id} ${picker.is_current_user ? '<span class="you-badge">You</span>' : ''}</td>
                <td>${ageDisplay}</td>
                <td>${picker.unique_picklists}</td>
                <td>${picker.items_picked}</td>
                <td>${picker.items_lost}</td>
                <td><strong>${picker.score}</strong></td>
                <td><span class="rank-badge ${picker.status_color}">${statusLabel}</span></td>
            </tr>
        `;
    }).join('');
    
    // Show current user section if they're not in top 15
    if (currentUserSection && currentUserBody) {
        if (!currentUserInTop15 && currentUserEntry) {
            currentUserSection.style.display = 'block';
            const statusLabel = getStatusLabel(currentUserEntry.status_color);
            const displayName = currentUserEntry.name || '-';
            const ageDisplay = currentUserEntry.age_in_days !== null && currentUserEntry.age_in_days !== undefined ? currentUserEntry.age_in_days : '-';
            
            currentUserBody.innerHTML = `
                <tr class="current-user-row">
                    <td><strong>#${currentUserEntry.rank}</strong></td>
                    <td>${displayName}</td>
                    <td>${currentUserEntry.picker_id} <span class="you-badge">You</span></td>
                    <td>${ageDisplay}</td>
                    <td>${currentUserEntry.unique_picklists}</td>
                    <td>${currentUserEntry.items_picked}</td>
                    <td>${currentUserEntry.items_lost}</td>
                    <td><strong>${currentUserEntry.score}</strong></td>
                    <td><span class="rank-badge ${currentUserEntry.status_color}">${statusLabel}</span></td>
                </tr>
            `;
        } else {
            currentUserSection.style.display = 'none';
        }
    }
}

function getStatusLabel(color) {
    if (color === 'green') return 'Going Good';
    if (color === 'yellow') return 'Can Do Better';
    return 'Need to Perform Better';
}
