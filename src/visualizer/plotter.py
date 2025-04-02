import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Union, Any


class Plotter:
    """
    Comprehensive financial data visualization module.
    Supports multiple chart types and layouts for technical analysis.
    """
    
    def __init__(self, figsize: Tuple[int, int] = (12, 8), style: str = 'dark_background'):
        """
        Initialize the Plotter with customizable figure settings.
        
        Args:
            figsize: Tuple specifying figure dimensions (width, height)
            style: Matplotlib style to use (e.g., 'dark_background', 'ggplot', etc.)
        """
        self.figsize = figsize
        self.style = style
        plt.style.use(style)
        
        # Default configuration
        self.config = {
            'candle_width': 0.8,
            'up_color': 'green',
            'down_color': 'red',
            'wick_color': '#000000',
            'volume_up_color': (0, 0.8, 0, 0.5),  # RGBA tuple
            'volume_down_color': (0.8, 0, 0, 0.5),  # RGBA tuple
            'grid': True,
            'date_format': '%H:%M',
            'title_fontsize': 14,
            'axis_fontsize': 12,
            'show_volume': True,
            'volume_panel_size': 0.2,  # Relative to main panel
            'tight_layout': True
        }
        
        # Initialize figure and axes
        self.fig = None
        self.axes = []
        
    def set_config(self, **kwargs) -> None:
        """
        Update configuration parameters.
        
        Args:
            **kwargs: Configuration parameters to update
        """
        self.config.update(kwargs)
        
    def _setup_subplots(self, n_charts: int, layout: str = 'vertical') -> None:
        """
        Set up the subplots based on the number of charts and layout.
        
        Args:
            n_charts: Number of price charts to display
            layout: 'vertical' or 'overlay' or 'grid'
        """
        if layout == 'vertical':
            if self.config['show_volume']:
                # Each chart gets its own volume panel
                total_panels = n_charts * 2
                heights = []
                for _ in range(n_charts):
                    heights.extend([1, self.config['volume_panel_size']])
                self.fig, self.axes = plt.subplots(total_panels, 1, 
                                                 figsize=self.figsize,
                                                 gridspec_kw={'height_ratios': heights},
                                                 sharex=True)
            else:
                self.fig, self.axes = plt.subplots(n_charts, 1, 
                                                 figsize=self.figsize,
                                                 sharex=True)
        
        elif layout == 'overlay':
            if self.config['show_volume']:
                self.fig, self.axes = plt.subplots(2, 1, 
                                                 figsize=self.figsize,
                                                 gridspec_kw={'height_ratios': [1, self.config['volume_panel_size']]},
                                                 sharex=True)
            else:
                self.fig, self.axes = plt.subplots(1, 1, figsize=self.figsize)
                self.axes = [self.axes]  # Ensure axes is a list
        
        elif layout == 'grid':
            # Calculate grid dimensions based on n_charts
            cols = min(3, n_charts)
            rows = (n_charts + cols - 1) // cols
            
            if self.config['show_volume']:
                self.fig = plt.figure(figsize=self.figsize)
                self.axes = []
                
                for i in range(n_charts):
                    # Create price and volume panels for each chart
                    row, col = divmod(i, cols)
                    
                    # Price panel
                    ax_price = plt.subplot2grid((rows * 2, cols), (row * 2, col))
                    
                    # Volume panel below price
                    ax_vol = plt.subplot2grid((rows * 2, cols), (row * 2 + 1, col), sharex=ax_price)
                    
                    self.axes.extend([ax_price, ax_vol])
            else:
                self.fig, self.axes = plt.subplots(rows, cols, figsize=self.figsize)
                self.axes = self.axes.flatten()
        
        else:
            raise ValueError(f"Unsupported layout: {layout}. Use 'vertical', 'overlay', or 'grid'.")
    
    def _preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure DataFrame has the correct format for plotting.
        
        Args:
            df: Input DataFrame with OHLCV data
            
        Returns:
            Preprocessed DataFrame
        """
        df = df.copy()
        
        # Convert time column to datetime if it's not already
        if 'time' in df.columns and not pd.api.types.is_datetime64_dtype(df['time']):
            if pd.api.types.is_numeric_dtype(df['time']):
                df['time'] = pd.to_datetime(df['time'], unit='ms')
            else:
                df['time'] = pd.to_datetime(df['time'])
        
        # If there's a 'datetime' column, use it; otherwise create it from 'time'
        if 'datetime' not in df.columns:
            if 'time' in df.columns:
                df['datetime'] = df['time']
            else:
                raise ValueError("DataFrame must have either 'time' or 'datetime' column")
        
        # Ensure required columns exist
        required_cols = ['open', 'high', 'low', 'close']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"DataFrame must contain all OHLC columns: {required_cols}")
        
        # Sort by datetime
        df = df.sort_values('datetime')
        
        return df
        
    def plot_candles(self, 
                     data_list: List[Union[pd.DataFrame, Dict[str, Any]]], 
                     titles: List[str] = None,
                     layout: str = 'vertical',
                     indicators: Dict[str, List[Dict]] = None,
                     show: bool = True,
                     save_path: str = None) -> Tuple[plt.Figure, List[plt.Axes]]:
        """
        Plot candlestick charts for multiple datasets.
        
        Args:
            data_list: List of DataFrames or dicts with OHLCV data
            titles: List of titles for each chart
            layout: 'vertical', 'overlay', or 'grid'
            indicators: Dict of technical indicators to plot
            show: Whether to display the plot
            save_path: Path to save the figure
            
        Returns:
            Tuple of (figure, axes)
        """
        # Convert any dict to DataFrame
        dfs = []
        for data in data_list:
            if isinstance(data, dict):
                df = pd.DataFrame(data)
            else:
                df = data
            dfs.append(self._preprocess_dataframe(df))
        
        # Set up titles
        if titles is None:
            titles = [f"Chart {i+1}" for i in range(len(dfs))]
        elif len(titles) < len(dfs):
            titles.extend([f"Chart {i+1}" for i in range(len(titles), len(dfs))])
        
        # Create figure and axes
        n_charts = len(dfs)
        self._setup_subplots(n_charts, layout)
        
        if layout == 'overlay':
            # Overlay all datasets on the same chart
            ax_price = self.axes[0]
            
            for i, df in enumerate(dfs):
                self._plot_single_candle_chart(df, ax_price, titles[i])
                
            if self.config['show_volume']:
                ax_vol = self.axes[1]
                for i, df in enumerate(dfs):
                    if 'volume' in df.columns:
                        self._plot_volume(df, ax_vol, alpha=0.7/len(dfs))
        else:
            # Plot each dataset on its own chart
            for i, df in enumerate(dfs):
                if i < len(self.axes):
                    ax_idx = i * 2 if self.config['show_volume'] else i
                    ax_price = self.axes[ax_idx]
                    self._plot_single_candle_chart(df, ax_price, titles[i])
                    
                    if self.config['show_volume'] and 'volume' in df.columns:
                        ax_vol = self.axes[ax_idx + 1]
                        self._plot_volume(df, ax_vol)
        
        # Add indicators if specified
        if indicators:
            self._add_indicators(dfs, indicators, layout)
        
        # Apply final formatting
        if self.config['tight_layout']:
            plt.tight_layout()
            
        # Save figure if path is provided
        if save_path:
            plt.savefig(save_path)
            
        # Show plot if requested
        if show:
            plt.show()
            
        return self.fig, self.axes
            
    def _plot_single_candle_chart(self, df: pd.DataFrame, ax: plt.Axes, title: str = None) -> None:
        """
        Plot a single candlestick chart on the given axis.
        
        Args:
            df: DataFrame with OHLCV data
            ax: Matplotlib axis to plot on
            title: Title for the chart
        """
        # Set title if provided
        if title:
            ax.set_title(title, fontsize=self.config['title_fontsize'])
        
        # Plot each candle
        width = self.config['candle_width'] / (24 * 60)  # Adjust based on time frequency
        
        for _, row in df.iterrows():
            # Determine if candle is bullish or bearish
            is_bullish = row['close'] >= row['open']
            color = self.config['up_color'] if is_bullish else self.config['down_color']
            
            # Plot candle body
            body_bottom = min(row['open'], row['close'])
            body_height = abs(row['close'] - row['open'])
            ax.bar(row['datetime'], body_height, bottom=body_bottom, width=width, color=color)
            
            # Plot candle wick
            ax.plot([row['datetime'], row['datetime']], 
                   [row['low'], row['high']], 
                   color=self.config['wick_color'], 
                   linewidth=1)
        
        # Set date formatter
        ax.xaxis.set_major_formatter(mdates.DateFormatter(self.config['date_format']))
        
        # Configure grid
        ax.grid(self.config['grid'])
        
        # Rotate x-axis labels for better readability
        plt.setp(ax.get_xticklabels(), rotation=45, ha='right')

    def _plot_volume(self, df: pd.DataFrame, ax: plt.Axes, alpha: float = 0.7) -> None:
        """
        Plot volume bars on the given axis.
        
        Args:
            df: DataFrame with OHLCV data
            ax: Matplotlib axis to plot on
            alpha: Transparency level
        """
        if 'volume' not in df.columns:
            ax.text(0.5, 0.5, 'No volume data available', 
                   horizontalalignment='center', 
                   verticalalignment='center',
                   transform=ax.transAxes)
            return
            
        # Plot each volume bar
        width = self.config['candle_width'] / (24 * 60)  # Adjust based on time frequency
        
        for _, row in df.iterrows():
            is_bullish = row['close'] >= row['open']
            color = self.config['volume_up_color'] if is_bullish else self.config['volume_down_color']
            
            ax.bar(row['datetime'], row['volume'], width=width, color=color, alpha=alpha)
        
        # Set y-axis format to human-readable numbers
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x/1000:.0f}K" if x < 1e6 else f"{x/1e6:.1f}M"))
        
        # Set label
        ax.set_ylabel('Volume', fontsize=self.config['axis_fontsize'])
        
        # Configure grid
        ax.grid(self.config['grid'], alpha=0.3)
        
        # Hide x tick labels if not the bottom subplot
        ax.tick_params(labelbottom=False)

    def plot_line(self, 
                 data_list: List[Union[pd.DataFrame, Dict[str, Any]]], 
                 value_column: str = 'close',
                 titles: List[str] = None,
                 layout: str = 'vertical',
                 show: bool = True,
                 save_path: str = None) -> Tuple[plt.Figure, List[plt.Axes]]:
        """
        Plot line charts for multiple datasets.
        
        Args:
            data_list: List of DataFrames or dicts with time series data
            value_column: Column to use for y-values
            titles: List of titles for each chart
            layout: 'vertical', 'overlay', or 'grid'
            show: Whether to display the plot
            save_path: Path to save the figure
            
        Returns:
            Tuple of (figure, axes)
        """
        # Convert any dict to DataFrame
        dfs = []
        for data in data_list:
            if isinstance(data, dict):
                df = pd.DataFrame(data)
            else:
                df = data
            dfs.append(self._preprocess_dataframe(df))
        
        # Set up titles
        if titles is None:
            titles = [f"Chart {i+1}" for i in range(len(dfs))]
        elif len(titles) < len(dfs):
            titles.extend([f"Chart {i+1}" for i in range(len(titles), len(dfs))])
        
        # Create figure and axes
        n_charts = len(dfs)
        self._setup_subplots(n_charts, layout)
        
        if layout == 'overlay':
            # Overlay all datasets on the same chart
            ax = self.axes[0]
            
            for i, df in enumerate(dfs):
                ax.plot(df['datetime'], df[value_column], label=titles[i])
                
            ax.set_title("Price Comparison", fontsize=self.config['title_fontsize'])
            ax.legend()
            
            if self.config['show_volume']:
                ax_vol = self.axes[1]
                for i, df in enumerate(dfs):
                    if 'volume' in df.columns:
                        self._plot_volume(df, ax_vol, alpha=0.7/len(dfs))
        else:
            # Plot each dataset on its own chart
            for i, df in enumerate(dfs):
                if i < len(self.axes):
                    ax_idx = i * 2 if self.config['show_volume'] else i
                    ax = self.axes[ax_idx]
                    
                    ax.plot(df['datetime'], df[value_column])
                    ax.set_title(titles[i], fontsize=self.config['title_fontsize'])
                    
                    if self.config['show_volume'] and 'volume' in df.columns:
                        ax_vol = self.axes[ax_idx + 1]
                        self._plot_volume(df, ax_vol)
        
        # Apply date formatting
        for ax in self.axes:
            ax.xaxis.set_major_formatter(mdates.DateFormatter(self.config['date_format']))
            ax.grid(self.config['grid'])
            plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
        
        # Apply final formatting
        if self.config['tight_layout']:
            plt.tight_layout()
            
        # Save figure if path is provided
        if save_path:
            plt.savefig(save_path)
            
        # Show plot if requested
        if show:
            plt.show()
            
        return self.fig, self.axes
    
    def _add_indicators(self, dfs: List[pd.DataFrame], indicators: Dict[str, List[Dict]], layout: str) -> None:
        """
        Add technical indicators to the charts.
        
        Args:
            dfs: List of DataFrames
            indicators: Dict of indicators to add
            layout: Chart layout
        """
        if not indicators:
            return
            
        if layout == 'overlay':
            ax_main = self.axes[0]
            
            for indicator_type, indicator_list in indicators.items():
                for indicator in indicator_list:
                    # Get data and params
                    df_idx = indicator.get('df_index', 0)
                    if df_idx < len(dfs):
                        df = dfs[df_idx]
                        
                        if indicator_type == 'ma':
                            self._add_moving_average(df, ax_main, indicator)
                        elif indicator_type == 'bollinger':
                            self._add_bollinger_bands(df, ax_main, indicator)
                        # Add more indicator types as needed
        else:
            for indicator_type, indicator_list in indicators.items():
                for indicator in indicator_list:
                    # Get data and params
                    df_idx = indicator.get('df_index', 0)
                    if df_idx < len(dfs):
                        df = dfs[df_idx]
                        
                        # In vertical/grid layout, determine the correct axis
                        ax_idx = df_idx * 2 if self.config['show_volume'] else df_idx
                        if ax_idx < len(self.axes):
                            ax = self.axes[ax_idx]
                            
                            if indicator_type == 'ma':
                                self._add_moving_average(df, ax, indicator)
                            elif indicator_type == 'bollinger':
                                self._add_bollinger_bands(df, ax, indicator)
                            # Add more indicator types as needed
    
    def _add_moving_average(self, df: pd.DataFrame, ax: plt.Axes, params: Dict) -> None:
        """
        Add moving average to chart.
        
        Args:
            df: DataFrame with price data
            ax: Matplotlib axis
            params: Parameters for the moving average
        """
        period = params.get('period', 20)
        column = params.get('column', 'close')
        color = params.get('color', 'cyan')
        label = params.get('label', f"{period}-period MA")
        
        # Calculate MA
        ma = df[column].rolling(window=period).mean()
        
        # Plot MA
        ax.plot(df['datetime'], ma, color=color, linewidth=1.5, label=label)
        
        # Update legend
        ax.legend(loc='upper left')
    
    def _add_bollinger_bands(self, df: pd.DataFrame, ax: plt.Axes, params: Dict) -> None:
        """
        Add Bollinger Bands to chart.
        
        Args:
            df: DataFrame with price data
            ax: Matplotlib axis
            params: Parameters for Bollinger Bands
        """
        period = params.get('period', 20)
        std_dev = params.get('std_dev', 2)
        column = params.get('column', 'close')
        ma_color = params.get('ma_color', 'cyan')
        band_color = params.get('band_color', 'gray')
        fill = params.get('fill', True)
        alpha = params.get('alpha', 0.2)
        
        # Calculate Bollinger Bands
        ma = df[column].rolling(window=period).mean()
        std = df[column].rolling(window=period).std()
        upper_band = ma + (std * std_dev)
        lower_band = ma - (std * std_dev)
        
        # Plot middle band (MA)
        ax.plot(df['datetime'], ma, color=ma_color, linewidth=1.5, label=f"BB ({period}, {std_dev})")
        
        # Plot upper and lower bands
        ax.plot(df['datetime'], upper_band, color=band_color, linestyle='--', linewidth=1)
        ax.plot(df['datetime'], lower_band, color=band_color, linestyle='--', linewidth=1)
        
        # Fill between bands if requested
        if fill:
            ax.fill_between(df['datetime'], lower_band, upper_band, color=band_color, alpha=alpha)
        
        # Update legend
        ax.legend(loc='upper left')
    
    def plot_heatmap(self, 
                    df: pd.DataFrame, 
                    value_column: str, 
                    x_column: str = None,
                    y_column: str = None,
                    title: str = 'Heatmap',
                    colormap: str = 'viridis',
                    show: bool = True,
                    save_path: str = None) -> Tuple[plt.Figure, plt.Axes]:
        """
        Plot a heatmap of values.
        
        Args:
            df: DataFrame with data
            value_column: Column containing values to plot
            x_column: Column for x-axis (defaults to index)
            y_column: Column for y-axis (defaults to columns)
            title: Title for the heatmap
            colormap: Matplotlib colormap name
            show: Whether to display the plot
            save_path: Path to save the figure
            
        Returns:
            Tuple of (figure, axes)
        """
        plt.style.use(self.style)
        fig, ax = plt.subplots(figsize=self.figsize)
        
        # Prepare data for heatmap
        if x_column and y_column:
            # Pivot the data
            pivot_df = df.pivot(index=y_column, columns=x_column, values=value_column)
        else:
            pivot_df = df
            
        # Plot heatmap
        im = ax.imshow(pivot_df.values, cmap=colormap, aspect='auto')
        
        # Set title
        ax.set_title(title, fontsize=self.config['title_fontsize'])
        
        # Set x and y tick labels
        ax.set_xticks(np.arange(len(pivot_df.columns)))
        ax.set_yticks(np.arange(len(pivot_df.index)))
        ax.set_xticklabels(pivot_df.columns)
        ax.set_yticklabels(pivot_df.index)
        
        # Rotate x tick labels
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")
        
        # Add colorbar
        cbar = fig.colorbar(im, ax=ax)
        cbar.set_label(value_column)
        
        # Apply final formatting
        plt.tight_layout()
        
        # Save figure if path is provided
        if save_path:
            plt.savefig(save_path)
            
        # Show plot if requested
        if show:
            plt.show()
            
        return fig, ax
    
    def close(self) -> None:
        """Close all matplotlib figures."""
        plt.close('all')


# Example usage function
def plot_example():
    # Create sample data
    dates = pd.date_range(start='2023-01-01', periods=100, freq='h')
    np.random.seed(42)
    
    # Create random OHLCV data with empty arrays
    data1 = {
        'datetime': dates,
        'open': np.random.normal(100, 2, 100),
        'high': np.zeros(100),
        'low': np.zeros(100),
        'close': np.zeros(100),
        'volume': np.random.randint(1000, 10000, 100)
    }
    
    # Fill in high, low, close based on open
    for i in range(len(data1['open'])):
        data1['close'][i] = data1['open'][i] + np.random.normal(0, 1)
        data1['high'][i] = max(data1['open'][i], data1['close'][i]) + abs(np.random.normal(0, 0.5))
        data1['low'][i] = min(data1['open'][i], data1['close'][i]) - abs(np.random.normal(0, 0.5))
    
    # Create second dataset with offset
    data2 = {
        'datetime': dates,
        'open': np.random.normal(150, 3, 100),
        'high': np.zeros(100),
        'low': np.zeros(100),
        'close': np.zeros(100),
        'volume': np.random.randint(5000, 15000, 100)
    }
    
    # Fill in high, low, close based on open
    for i in range(len(data2['open'])):
        data2['close'][i] = data2['open'][i] + np.random.normal(0, 1.2)
        data2['high'][i] = max(data2['open'][i], data2['close'][i]) + abs(np.random.normal(0, 0.7))
        data2['low'][i] = min(data2['open'][i], data2['close'][i]) - abs(np.random.normal(0, 0.7))
    
    # Convert to DataFrames
    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)
    
    # Create Plotter
    plotter = Plotter(figsize=(14, 10), style='dark_background')
    
    # Configure plotter
    plotter.set_config(
        up_color='lime',
        down_color='red',
        candle_width=0.6,
        date_format='%m-%d %H:%M',
        show_volume=True
    )
    
    # Plot candles in vertical layout
    plotter.plot_candles(
        [df1, df2],
        titles=['Asset 1', 'Asset 2'],
        layout='vertical',
        indicators={
            'ma': [
                {'df_index': 0, 'period': 20, 'color': 'yellow', 'label': '20-period MA'},
                {'df_index': 1, 'period': 10, 'color': 'cyan', 'label': '10-period MA'}
            ],
            'bollinger': [
                {'df_index': 0, 'period': 20, 'std_dev': 2}
            ]
        },
        show=True,
        save_path='vertical_candles.png'
    )
    
    # Plot candles in overlay layout
    plotter.plot_candles(
        [df1, df2],
        titles=['Asset 1', 'Asset 2'],
        layout='overlay',
        show=True
    )
    
    # Plot as line chart
    plotter.plot_line(
        [df1, df2],
        value_column='close',
        titles=['Asset 1', 'Asset 2'],
        layout='overlay',
        show=True
    )
    
    # Clean up
    plotter.close()
