import aiosqlite
import asyncio
import pandas as pd
import matplotlib.pyplot as plt
import os

async def generate_chart():
    db_path = "gold_bot.sqlite"
    if not os.path.exists(db_path):
        print(f"Baza topilmadi: {db_path}. Bot biroz ishlagandan so'ng qayta urinib ko'ring.")
        return
        
    async with aiosqlite.connect(db_path) as db:
        async with db.execute("SELECT opened_at, result, risk_usd FROM trades WHERE result IS NOT NULL") as cursor:
            rows = await cursor.fetchall()
            
    if not rows:
        print("Hali yopilgan bitimlar yo'q.")
        return

    df = pd.DataFrame(rows, columns=["date", "result", "risk_usd"])
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    def calc_pnl(row):
        res = str(row['result'])
        risk = float(row['risk_usd'])
        if 'TP3' in res: return risk * 4.5
        elif 'TP2' in res: return risk * 3.0
        elif 'TP1' in res: return risk * 1.5
        elif 'SL' in res: return -risk
        return 0.0
        
    df['pnl'] = df.apply(calc_pnl, axis=1)
    df['cumulative_pnl'] = df['pnl'].cumsum()
    
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(10, 5))
    
    # Gradient osti bo'yalgan qism
    ax.plot(df['date'], df['cumulative_pnl'], color='#f0b90b', linewidth=2.5, marker='o', markersize=5)
    ax.fill_between(df['date'], df['cumulative_pnl'], color='#f0b90b', alpha=0.15)
    
    ax.set_title('GOLD SMC Bot - Daromad O\'sish Dinamikasi (USD)', fontsize=14, pad=20, color='white', fontweight='bold')
    ax.set_xlabel('Vaqt', fontsize=11, color='#8b949e')
    ax.set_ylabel('Foyda / Zarar ($)', fontsize=11, color='#8b949e')
    
    ax.grid(color='#30363d', linestyle='--', linewidth=0.5)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#30363d')
    ax.spines['bottom'].set_color('#30363d')
    
    plt.tight_layout()
    os.makedirs("assets", exist_ok=True)
    plt.savefig("assets/winrate_chart.png", dpi=300, bbox_inches='tight', transparent=True)
    print("Muvaffaqiyatli saqlandi! Rasmni ko'rishingiz mumkin: assets/winrate_chart.png")

if __name__ == "__main__":
    asyncio.run(generate_chart())
