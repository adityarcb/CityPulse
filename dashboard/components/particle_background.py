"""
CityPulse — Living Atmosphere Canvas Particle Background Component

Full-screen HTML5 Canvas particle background.
Adapts density, speed, and color based on real-time average PM2.5:
- <100 µg/m³: Sparse slow teal particles (clean atmospheric drift)
- 100-150 µg/m³: Amber haze with moderate motion
- ≥150 µg/m³: Dense fast crimson smog particles
Features:
- Capped at ≤140 particles for 60fps performance
- Automatically pauses when browser tab is hidden (document.hidden)
- Respects user accessibility preferences: window.matchMedia('(prefers-reduced-motion: reduce)')
- Non-blocking pointer-events: none (zero interaction interference)
"""

import streamlit as st


def render_particle_background(avg_pm25: float = 65.0):
    """Render the Living Atmosphere canvas particle layer."""
    # Determine particle profile based on average PM2.5
    if avg_pm25 < 100.0:
        color = "45, 212, 191"   # Teal
        count = 45
        speed_factor = 0.5
        label = "Clean Atmospheric Flow"
    elif avg_pm25 < 150.0:
        color = "245, 158, 11"   # Amber
        count = 85
        speed_factor = 0.9
        label = "Moderate Smog Haze"
    else:
        color = "244, 63, 94"    # Crimson
        count = 135
        speed_factor = 1.4
        label = "Severe Smog Inversion"

    canvas_html = f"""
    <div id="citypulse-atmosphere-wrapper" style="position:fixed; top:0; left:0; width:100vw; height:100vh; pointer-events:none; z-index:0; overflow:hidden;">
        <canvas id="citypulse-particles" style="display:block; width:100%; height:100%;"></canvas>
    </div>
    <script>
    (function() {{
        const canvas = document.getElementById('citypulse-particles');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        let width = canvas.width = window.innerWidth;
        let height = canvas.height = window.innerHeight;

        const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
        if (prefersReducedMotion) return; // Honor accessibility

        window.addEventListener('resize', () => {{
            if (!canvas) return;
            width = canvas.width = window.innerWidth;
            height = canvas.height = window.innerHeight;
        }});

        const particleCount = {count};
        const speedMultiplier = {speed_factor};
        const rgbColor = "{color}";

        const particles = [];
        for (let i = 0; i < particleCount; i++) {{
            particles.push({{
                x: Math.random() * width,
                y: Math.random() * height,
                radius: Math.random() * 2.2 + 0.8,
                vx: (Math.random() - 0.5) * 0.7 * speedMultiplier,
                vy: (Math.random() * 0.6 + 0.2) * -1 * speedMultiplier, // Gentle upward atmospheric drift
                alpha: Math.random() * 0.45 + 0.15,
                pulse: Math.random() * Math.PI,
            }});
        }}

        let isRunning = true;
        document.addEventListener('visibilitychange', () => {{
            isRunning = !document.hidden;
            if (isRunning) requestAnimationFrame(loop);
        }});

        function loop() {{
            if (!isRunning) return;
            ctx.clearRect(0, 0, width, height);

            for (let i = 0; i < particles.length; i++) {{
                const p = particles[i];
                p.x += p.vx;
                p.y += p.vy;
                p.pulse += 0.02;

                // Wrap around edges
                if (p.x < -10) p.x = width + 10;
                if (p.x > width + 10) p.x = -10;
                if (p.y < -10) p.y = height + 10;
                if (p.y > height + 10) p.y = -10;

                const currentAlpha = p.alpha + Math.sin(p.pulse) * 0.1;
                ctx.beginPath();
                ctx.arc(p.x, p.y, p.radius, 0, Math.PI * 2);
                ctx.fillStyle = 'rgba(' + rgbColor + ', ' + Math.max(0.05, Math.min(0.65, currentAlpha)) + ')';
                ctx.shadowBlur = 6;
                ctx.shadowColor = 'rgba(' + rgbColor + ', 0.4)';
                ctx.fill();
            }}

            requestAnimationFrame(loop);
        }}

        requestAnimationFrame(loop);
    }})();
    </script>
    """
    st.html(canvas_html)
