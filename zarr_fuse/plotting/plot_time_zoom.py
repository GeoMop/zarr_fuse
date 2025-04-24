import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime
from .store_overview import get_key_for_value


# Class to handle zoom and synchronization
class MultiZoomer:
    def __init__(self, df, data_selector, handlers):
        self.handlers = handlers
        self.handlers.append(self)
        self.data_selector = data_selector
        self.time_coord = get_key_for_value(self.data_selector, 'time_axis')
        self.lon_coord = get_key_for_value(self.data_selector, 'lon_axis')
        self.lat_coord = get_key_for_value(self.data_selector, 'lat_axis')

        self.df_full = df

        self.fig, self.axes = plt.subplots(
            len(self.quantities), 3, figsize=(15, 6),
            constrained_layout=True, sharey='row', sharex='col')


        # Instantiate the multi-zoom handler
        plt.show()
        self.cid = self.fig.canvas.mpl_connect('button_press_event', self.onclick)


        self.spans = [365, 30, 2]

        self.update_cross()


    @property
    def quantities(self):
        return [col
                for col in self.df_full.columns
                if col in self.data_selector and not isinstance(self.data_selector[col], str)
                ]  # Exclude 'time', 'lon', 'lat'.

    @property
    def colormaps(self):
        return self.data_selector

    def update_cross(self):
        if self.data_selector['lon_point'] is None:
            self.data_selector['lon_point'] = self.df_full[self.lon_coord][0]
        if self.data_selector['lat_point'] is None:
            self.data_selector['lat_point'] = self.df_full[self.lat_coord][0]

        mask_lon = self.df_full[self.lon_coord] == self.data_selector['lon_point']
        mask_lat = self.df_full[self.lat_coord] == self.data_selector['lat_point']
        self.df_sel = self.df_full.filter(mask_lon & mask_lat)


        # Plot the data in each axis
        # Plot data with different spans
        for ax_row, col in zip(self.axes, self.quantities):
            for ax in ax_row:
                ax.clear()
                df_col = self.df_sel[col]
                ax.plot(mdates.date2num(self.df_sel[self.time_coord]),
                        df_col)
                range = df_col.min(), df_col.max()

                assert range[0] < range[1], f"Col={col}, Invalid range: {range}"
                ax.set_ylim(*range)
                ax.grid()
                # print("X range", ax.get_xlim())
            ax_row[0].set_ylabel(col)

        labels = ["Year", "Month", "Day"]
        for i, ax in enumerate(self.axes[0]):
            ax.set_title(labels[i])

        self.x_range = self.axes[0, 0].get_xlim()
        range = max(self.x_range[1] - self.x_range[0], 365.0)
        self.spans = range * np.array([365, 30, 1]) / 365.0
        # print("Range:", self.x_range)
        self.x_center = (self.x_range[0] + self.x_range[1]) / 2

        self.update()

    def update(self):
        # Update each plot to center around the clicked time point
        for ax_row in self.axes:
            for ax, span in zip(ax_row, self.spans):
                # Adjust limits while respecting the data range
                new_xmin = max(self.x_range[0], self.x_center - span/2)
                new_xmax = min(self.x_range[1], self.x_center + span/2)
                #print(new_xmin, new_xmax, span)
                ax.set_xlim(new_xmin, new_xmax)
            # Custom tick locators and formatters
            # axes[0]: Month minor ticks for month starts, month shortcut labels, major tick for year start
            ax_row[0].xaxis.set_major_locator(mdates.YearLocator())  # Major ticks at year start
            ax_row[0].xaxis.set_major_formatter(mdates.DateFormatter('%Y'))  # Major tick format: Year
            ax_row[0].xaxis.set_minor_locator(mdates.MonthLocator())  # Minor ticks at month start
            ax_row[0].xaxis.set_minor_formatter(mdates.DateFormatter('%b'))  # Minor tick format: Month shortcut
            ax_row[0].tick_params(axis='x', which='minor', rotation=45, labelsize=8, pad=15)  # Rotate minor tick labels
    
            # ax_row[1]: Major tick for month start, minor tick for days, labeled by day in month
            ax_row[1].xaxis.set_major_locator(mdates.MonthLocator())  # Major ticks at month start
            ax_row[1].xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))  # Major tick format: Month and year
            ax_row[1].xaxis.set_minor_locator(mdates.DayLocator())  # Minor ticks at day start
            ax_row[1].xaxis.set_minor_formatter(mdates.DateFormatter('%d'))  # Minor tick format: Day in month
            ax_row[1].tick_params(axis='x', which='minor', labelsize=7)  # Rotate minor tick labels

            # ax_row[2]: Major tick for day, minor tick for hours
            ax_row[2].xaxis.set_major_locator(mdates.DayLocator())  # Major ticks at day start
            ax_row[2].xaxis.set_major_formatter(mdates.DateFormatter('%d %b'))  # Major tick format: Day and month
            ax_row[2].xaxis.set_minor_locator(mdates.HourLocator())  # Minor ticks every hour
            ax_row[2].xaxis.set_minor_formatter(mdates.DateFormatter('%H'))  # Minor tick format: Hour
            ax_row[2].tick_params(axis='x', which='minor', labelsize=7)  # Rotate minor tick labels

        # Redraw the canvas
        self.fig.canvas.draw()

    def onclick(self, event):
        #print(event.inaxes)
        #if event.inaxes not in self.axes:
        #    return
        # Determine the zoom factor based on the button clicked
        if event.button == 1 and event.xdata is not None:  # Left click to zoom in
            self.x_center = event.xdata
            for h in self.handlers:
                if h is self:
                    h.update()
                else:
                    h.update_cross()



#################################
if __name__ == '__main__':
    # Generate some early data
    dates = [datetime.datetime(2023, 1, 1) + datetime.timedelta(days=i, hours=j) for i in range(365) for j in range(24)]
    x  = np.linspace(0.0, 365.0, len(dates))
    #data = np.sin(2 * np.pi * x/365) + np.sin(2 * np.pi * (x % 365))
    data =  x / 365 + np.sin(2 * np.pi * (x % 365))
    df = pd.DataFrame({'time':dates, 'val':data, 'v1':2*data})
    df = df.set_index('time')

    zoom_plot_df(df)


