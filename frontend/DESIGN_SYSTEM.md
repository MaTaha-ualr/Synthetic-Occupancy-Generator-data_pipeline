# SOG Benchmark Studio - Design System

A production-ready design system for the Synthetic Occupancy Generator frontend.

---

## 🎨 Design Philosophy

### Visual Identity
- **Modern & Professional**: Clean lines, purposeful whitespace, refined interactions
- **Scientific Precision**: Monospace fonts for data, clear visual hierarchy
- **Approachable**: Warm accent colors (amber) balance the technical indigo primary

### Design Influences
- Linear.app (precision & clarity)
- Notion (approachable density)
- Vercel (modern developer tools)
- Stripe (polished interactions)

---

## 📐 Color System

### Neutral: Slate
Used for text, backgrounds, and UI chrome.

| Token | Value | Usage |
|-------|-------|-------|
| `--slate-50` | `#f8fafc` | Page background |
| `--slate-100` | `#f1f5f9` | Card backgrounds |
| `--slate-200` | `#e2e8f0` | Borders, dividers |
| `--slate-500` | `#64748b` | Secondary text |
| `--slate-700` | `#334155` | Primary text |
| `--slate-900` | `#0f172a` | Hero backgrounds |

### Primary: Indigo
Used for primary actions, active states, and brand elements.

| Token | Value | Usage |
|-------|-------|-------|
| `--indigo-500` | `#6366f1` | Primary buttons |
| `--indigo-600` | `#4f46e5` | Button hover |
| `--indigo-100` | `#e0e7ff` | Light backgrounds |

### Accent: Amber
Used for highlights, warnings, and secondary emphasis.

| Token | Value | Usage |
|-------|-------|-------|
| `--amber-400` | `#fbbf24` | Highlights |
| `--amber-500` | `#f59e0b` | Warnings, active states |

### Semantic Colors
- **Success**: `#10b981` (green)
- **Warning**: `#f59e0b` (amber)
- **Error**: `#ef4444` (red)
- **Info**: `#3b82f6` (blue)

---

## 🔤 Typography

### Font Stack
```css
/* Primary */
font-family: "Inter", -apple-system, BlinkMacSystemFont, sans-serif;

/* Monospace (data, code) */
font-family: "JetBrains Mono", "Fira Code", monospace;
```

### Type Scale

| Level | Size | Weight | Usage |
|-------|------|--------|-------|
| Display | 3.5rem (56px) | 800 | Hero headlines |
| H1 | 1.875rem (30px) | 700 | Page titles |
| H2 | 1.25rem (20px) | 700 | Section headers |
| H3 | 1rem (16px) | 600 | Card titles |
| Body | 0.9375rem (15px) | 400 | Paragraphs |
| Small | 0.875rem (14px) | 400 | Secondary text |
| Caption | 0.75rem (12px) | 500 | Labels, badges |
| Mono | 0.8125rem (13px) | 400 | Code, data |

---

## 🧩 Spacing System

Based on 4px increments:

| Token | Value |
|-------|-------|
| `--space-1` | 0.25rem (4px) |
| `--space-2` | 0.5rem (8px) |
| `--space-3` | 0.75rem (12px) |
| `--space-4` | 1rem (16px) |
| `--space-5` | 1.25rem (20px) |
| `--space-6` | 1.5rem (24px) |
| `--space-8` | 2rem (32px) |
| `--space-10` | 2.5rem (40px) |
| `--space-12` | 3rem (48px) |

---

## 🎯 Components

### Hero Section
- Dark gradient background (slate-900 to indigo-950)
- Subtle grid pattern overlay
- Gradient text accent (indigo to amber)
- Stats row with uppercase labels
- Glassmorphism status card

### Feature Cards
- White background with slate-200 border
- 3px gradient top border on hover
- Icon in gradient container
- Example code block with left accent
- Smooth hover lift animation

### Status Card
- Glassmorphism effect (blur + transparency)
- Animated status badge with pulse
- 2-column grid for stats
- Monospace labels

### Progress Card
- Gradient top border
- Percentage badge
- Animated progress bar
- Stage indicator

### Tags
- Rounded pills with icon + text
- Category colors:
  - Default: Indigo tint
  - Amber: For warnings/high noise

---

## ✨ Interactions

### Hover States
```css
/* Cards */
transform: translateY(-2px);
box-shadow: var(--shadow-lg);
border-color: var(--indigo-200);

/* Buttons */
transform: translateY(-1px);
box-shadow: 0 6px 20px rgba(79, 70, 229, 0.4);

/* Links */
color: var(--indigo-600);
```

### Transitions
- Default: `all 0.2s ease`
- Progress bars: `width 0.3s ease`

### Animations
- Status pulse: 2s infinite opacity cycle
- Progress fill: Smooth width transition

---

## 📱 Responsive Breakpoints

| Breakpoint | Width | Adjustments |
|------------|-------|-------------|
| Desktop | > 968px | Full layout |
| Mobile | ≤ 968px | Stack grids, reduce padding |

---

## 🖼️ Visual Assets

### Logo
SVG-based with gradient fill. Represents:
- Two overlapping entities (entity resolution)
- Connection/relationship visualization
- Small amber dot for "active" state

### Icons
Use emoji for simplicity and cross-platform compatibility:
- 🧬 Logo/mark
- ⚡ Quick actions
- ⚙️ Configure
- 📊 Analyze
- 📦 Export
- 🔐 Security
- 💬 Chat
- 🔀 Scenario

### Background Patterns
- CSS grid pattern (40px cells)
- Radial gradients for glow effects
- Subtle noise texture (optional)

---

## 🚀 Implementation Notes

### CSS Architecture
- CSS custom properties for theming
- Mobile-first responsive design
- BEM-like naming: `.sog-{component}-{element}`

### Performance
- Single CSS injection via `st.markdown()`
- SVG icons inlined (no external requests)
- GPU-accelerated transforms

### Accessibility
- WCAG AA color contrast ratios
- Focus states on interactive elements
- Semantic HTML structure

---

## 🔄 Migration from Old Design

| Old | New |
|-----|-----|
| Paper/warm background | Slate cool background |
| Navy hero | Slate-900 hero |
| Sora font | Inter font |
| Copper accent | Amber accent |
| `sog-` prefix kept | Same prefix, new styles |

---

## 📚 Files

- `chatbot_production.py` - New main app
- `visualizations/theme.py` - Color system
- `DESIGN_SYSTEM.md` - This documentation
