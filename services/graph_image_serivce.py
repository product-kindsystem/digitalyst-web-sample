import io
import base64
import datetime
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


class GraphImageService:


    @staticmethod
    def get_graph_image_base64(x_values, y_values, y2_values=None, width=1000, height=150, show_tick=False, y_max=None, line_color="#2196F3", bg_color="transparent"):
        if not x_values or not y_values:
            return ""

        dpi=100
        fig, ax = plt.subplots(figsize=(width/dpi, height/dpi), dpi=dpi)

        if bg_color != "transparent":
            fig.patch.set_facecolor(bg_color)
            ax.set_facecolor(bg_color)
            transparent = False
        else:
            fig.patch.set_alpha(0)
            ax.set_facecolor((0, 0, 0, 0))
            transparent = True
        
        # 2本目の線（y2_valuesがある場合）
        if y2_values is not None:
            ax.plot(x_values, y2_values, color="#FFB74D")  # オレンジ系　#FFB74D　黄緑系 #8BC34A

        # 折れ線グラフを描画
        ax.plot(x_values, y_values, color=line_color)
        
        # X軸の範囲とフォーマット
        ax.set_xlim(min(x_values), max(x_values))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        
        # Y軸の最大値を設定（最小値は自動）
        if y_max is not None:
            ax.set_ylim(bottom=0, top=y_max)
        
        # 枠線を一部非表示、残りはグレー
        ax.spines["top"].set_visible(False)
        ax.spines["left"].set_visible(False)
        for spine in ax.spines.values():
            spine.set_color("#888888")

        if show_tick:
            # X軸 目盛りあり
            # Y軸非表示、X軸目盛り色をグレーに
            ax.yaxis.set_visible(False)
            ax.tick_params(axis='x', labelrotation=0, colors="#888888", labelsize=8)
            
            # 余白調整
            fig.subplots_adjust(left=0, right=1, bottom=0.2, top=1)
        else:
            # X軸 目盛りなし
            # Y軸非表示、X軸目盛り色をグレーに
            ax.yaxis.set_visible(False)
            ax.xaxis.set_visible(False)
            
            # 余白調整
            fig.subplots_adjust(left=0, right=1, bottom=0, top=1)

        buf = io.BytesIO()
        fig.savefig(buf, format="png", transparent=transparent)
        plt.close(fig)
        buf.seek(0)
        return base64.b64encode(buf.read()).decode("utf-8")
