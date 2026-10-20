package com.hca.monitor.notify;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Color;
import java.awt.Desktop;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.SystemTray;
import java.awt.TrayIcon;
import java.awt.image.BufferedImage;
import java.net.URI;

/**
 * Windows notifications through the system tray: a small icon sits in the tray while the engine runs and each
 * failure pops a notification. Clicking the notification opens its link (AWS console or dashboard).
 * On a server with no desktop (e.g. EC2) this switches itself off and says so once.
 */
public class DesktopNotifier implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DesktopNotifier.class);

    private TrayIcon icon;
    private volatile String lastLink;
    private boolean unavailableLogged;

    public synchronized boolean show(String title, String text, boolean failure, String link) {
        if (!ready()) return false;
        lastLink = link;
        icon.displayMessage(title, text, failure ? TrayIcon.MessageType.ERROR : TrayIcon.MessageType.INFO);
        return true;
    }

    private boolean ready() {
        if (icon != null) return true;
        try {
            if (java.awt.GraphicsEnvironment.isHeadless() || !SystemTray.isSupported()) {
                unavailable("this machine has no desktop / system tray");
                return false;
            }
            icon = new TrayIcon(drawIcon(), "Pipeline monitor");
            icon.setImageAutoSize(true);
            icon.addActionListener(e -> open(lastLink));    // click on the notification
            SystemTray.getSystemTray().add(icon);
            log.info("Desktop notifications on (tray icon 'Pipeline monitor')");
            return true;
        } catch (Throwable t) {
            icon = null;
            unavailable(t.toString());
            return false;
        }
    }

    private void unavailable(String why) {
        if (!unavailableLogged) {
            unavailableLogged = true;
            log.warn("Desktop notifications unavailable: {} - set monitor.notify.desktop-enabled: false to hide this", why);
        }
    }

    private static void open(String link) {
        if (link == null || link.isBlank()) return;
        try {
            if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
                Desktop.getDesktop().browse(new URI(link));
            } else {
                new ProcessBuilder("rundll32", "url.dll,FileProtocolHandler", link).start();
            }
        } catch (Exception e) {
            log.debug("Could not open {}: {}", link, e.toString());
        }
    }

    /** A small four-bar "batch" glyph so the tray icon is recognisable. */
    private static BufferedImage drawIcon() {
        BufferedImage img = new BufferedImage(32, 32, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = img.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        Color[] c = {new Color(0x2E7A4C), new Color(0x2E7A4C), new Color(0x2265A8), new Color(0xB4322B)};
        for (int i = 0; i < 4; i++) {
            g.setColor(c[i]);
            g.fillRect(2 + i * 7, 10, 6, 12);
        }
        g.dispose();
        return img;
    }

    @Override
    public synchronized void close() {
        try {
            if (icon != null) SystemTray.getSystemTray().remove(icon);
        } catch (Throwable ignored) {
            // shutting down
        }
        icon = null;
    }
}
