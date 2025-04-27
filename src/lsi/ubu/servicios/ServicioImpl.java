package lsi.ubu.servicios;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.excepciones.AlquilerCochesException;
import lsi.ubu.util.PoolDeConexiones;

public class ServicioImpl implements Servicio {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServicioImpl.class);

	private static final int DIAS_DE_ALQUILER = 4;

	public void alquilar(String nifCliente, String matricula, Date fechaIni, Date fechaFin) throws SQLException {
		PoolDeConexiones pool = PoolDeConexiones.getInstance();

		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;

		/*
		 * El calculo de los dias se da hecho
		 */
		long diasDiff = DIAS_DE_ALQUILER;
		if (fechaFin != null) {
			diasDiff = TimeUnit.MILLISECONDS.toDays(fechaFin.getTime() - fechaIni.getTime());

			if (diasDiff < 1) {
				throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS);
			}
		}

		try {
			con = pool.getConnection();

			/* A completar por el alumnado... */

			/* ================================= AYUDA RÁPIDA ===========================*/
			/*
			 * Algunas de las columnas utilizan tipo numeric en SQL, lo que se traduce en
			 * BigDecimal para Java.
			 * 
			 * Convertir un entero en BigDecimal: new BigDecimal(diasDiff)
			 * 
			 * Sumar 2 BigDecimals: usar metodo "add" de la clase BigDecimal
			 * 
			 * Multiplicar 2 BigDecimals: usar metodo "multiply" de la clase BigDecimal
			 *
			 * 
			 * Paso de util.Date a sql.Date java.sql.Date sqlFechaIni = new
			 * java.sql.Date(sqlFechaIni.getTime());
			 *
			 *
			 * Recuerda que hay casos donde la fecha fin es nula, por lo que se debe de
			 * calcular sumando los dias de alquiler (ver variable DIAS_DE_ALQUILER) a la
			 * fecha ini.
			 */
			con.setAutoCommit(false); // Auto Commit off para no tener sorpresas
            
            // 1. Verificar si el cliente existe
            st = con.prepareStatement("SELECT COUNT(*) FROM clientes WHERE NIF = ?");
            st.setString(1, nifCliente);
            rs = st.executeQuery();
            if (rs.next() && rs.getInt(1) == 0) {
                throw new AlquilerCochesException(AlquilerCochesException.CLIENTE_NO_EXIST);
            }
            
            // 2. Verificar si el vehículo existe
            st = con.prepareStatement("SELECT COUNT(*) FROM vehiculos WHERE matricula = ?");
            st.setString(1, matricula);
            rs = st.executeQuery();
            
            if (rs.next() && rs.getInt(1) == 0) {
                throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_NO_EXIST);
            }
            
            // 3. Verificar disponibilidad del vehículo
            st = con.prepareStatement(
                "SELECT COUNT(*) FROM reservas WHERE matricula = ? " +
                "AND ((fecha_ini BETWEEN ? AND ?) OR (fecha_fin BETWEEN ? AND ?))");
            
            
            java.sql.Date sqlFechaFin = fechaFin != null ? 
                    new java.sql.Date(fechaFin.getTime()) : null;
            
            java.sql.Date sqlFechaIni = new java.sql.Date(fechaIni.getTime());
            
            st.setString(1, matricula);
            st.setDate(2, sqlFechaIni);
            st.setDate(3, sqlFechaFin);
            st.setDate(4, sqlFechaIni);
            st.setDate(5, sqlFechaFin);
            
            rs = st.executeQuery();
           

            if (rs.next() && rs.getInt(1) > 0) {
            	throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_OCUPADO);
            }
            
            // 4. Insertar la reserva
            st = con.prepareStatement(
                "INSERT INTO reservas (idReserva, cliente, matricula, fecha_ini, fecha_fin) " +
                "VALUES (seq_reservas.nextval, ?, ?, ?, ?)");
            
            st.setString(1, nifCliente);
            st.setString(2, matricula);
            st.setDate(3, sqlFechaIni);
            st.setDate(4, sqlFechaFin);
            
            st.executeUpdate();
            
            // Obtenemos el valor por dia del coche, capacidad del deposito y el precio
            st = con.prepareStatement(
                    "SELECT m.precio_cada_dia, m.capacidad_deposito, pc.precio_por_litro, m.id_modelo " +
                    "FROM modelos m " +
                    "JOIN vehiculos v ON m.id_modelo = v.id_modelo " +
                    "JOIN precio_combustible pc ON m.tipo_combustible = pc.tipo_combustible " +
                    "WHERE v.matricula = ?");
                
            st.setString(1, matricula);
            rs = st.executeQuery();
            rs.next();
            BigDecimal precioPorDia = rs.getBigDecimal("precio_cada_dia");
            int capacidadDeposito = rs.getInt("capacidad_deposito");
            BigDecimal precioPorLitro = rs.getBigDecimal("precio_por_litro");
            int modelo_coche = rs.getInt("id_modelo");
                
            BigDecimal importeTotalLinea = precioPorDia.multiply(new BigDecimal(diasDiff));
            BigDecimal CosteGasolina = precioPorLitro.multiply(new BigDecimal(capacidadDeposito));
            BigDecimal importeTotalFactura = CosteGasolina.add(importeTotalLinea);
            
            // 5. Insertar la factura
            st = con.prepareStatement(
            	    "INSERT INTO Facturas (nroFactura, importe, cliente) " +
            	    "VALUES (seq_num_fact.NEXTVAL, ?, ?)");
            	    
        	st.setBigDecimal(1, importeTotalFactura);
        	st.setString(2, nifCliente);
        	st.executeUpdate();

        	// obtenemos el valor actual de la secuencia con CURRVAL
        	st= con.prepareStatement(
        	    "SELECT seq_num_fact.CURRVAL FROM dual");
        	rs = st.executeQuery();
            
            rs.next();
            BigDecimal nroFactura = rs.getBigDecimal(1);
            
		} catch (SQLException e) {
			if (con != null) {
                try {
                    con.rollback(); // Si ocurre un error hacemos rollback
                } catch (SQLException e1) {
                    LOGGER.error("Error al hacer rollback");
                }
            }
            
            if (e instanceof AlquilerCochesException) {
                throw e;
            } else {
                LOGGER.error(e.getMessage());
                throw e;
            }


		} finally {
			// Cerramos los recursos abiertos
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    LOGGER.error("Error al cerrar ResultSet", e);
                }
            }
            if (st != null) {
                try {
                    st.close();
                } catch (SQLException e) {
                    LOGGER.error("Error al cerrar PreparedStatement", e);
                }
            }
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    LOGGER.error("Error al cerrar Connection", e);
                }
            }
		}
	}
}
